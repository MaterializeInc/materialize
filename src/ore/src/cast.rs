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
        impl CastFrom<$from> for $to {
            fn cast_from(from: $from) -> $to {
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

#[cfg(any(target_pointer_width = "32", target_pointer_width = "64"))]
cast_from!(i32, isize);

#[cfg(target_pointer_width = "64")]
cast_from!(i64, isize);

cast_from!(isize, i64);

#[cfg(any(target_pointer_width = "32", target_pointer_width = "64"))]
cast_from!(isize, i128);

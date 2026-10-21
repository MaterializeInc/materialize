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

//! The numeric conversions the tracker needs, in the style of
//! `mz_ore::cast`. They are local because `mz-ore` depends on this crate.

#![allow(clippy::as_conversions)]

#[cfg(not(target_pointer_width = "64"))]
compile_error!("the allocation tracker assumes 64-bit pointers");

/// A lossless conversion.
pub(crate) trait CastFrom<T> {
    fn cast_from(from: T) -> Self;
}

// `usize` and `u64` have the same width on the 64-bit targets this crate
// compiles for, which the `compile_error!` above enforces.
impl CastFrom<usize> for u64 {
    #[inline]
    fn cast_from(from: usize) -> u64 {
        from as u64
    }
}

impl CastFrom<u64> for usize {
    #[inline]
    fn cast_from(from: u64) -> usize {
        from as usize
    }
}

impl CastFrom<u32> for usize {
    #[inline]
    fn cast_from(from: u32) -> usize {
        from as usize
    }
}

/// A conversion that may round or saturate.
pub(crate) trait CastLossy<T> {
    fn cast_lossy(from: T) -> Self;
}

impl CastLossy<usize> for f64 {
    #[inline]
    fn cast_lossy(from: usize) -> f64 {
        from as f64
    }
}

impl CastLossy<u64> for f64 {
    #[inline]
    fn cast_lossy(from: u64) -> f64 {
        from as f64
    }
}

/// Saturates at the bounds of `u64` and maps NaN to zero.
impl CastLossy<f64> for u64 {
    #[inline]
    fn cast_lossy(from: f64) -> u64 {
        from as u64
    }
}

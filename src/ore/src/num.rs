// Copyright 2019 The Rust Project Contributors
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

//! Number utilities

use num::traits::bounds::UpperBounded;
use num::Signed;
#[cfg(feature = "proptest")]
use proptest::arbitrary::Arbitrary;
#[cfg(feature = "proptest")]
use proptest::strategy::{BoxedStrategy, Strategy};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::ops::Deref;
#[cfg(feature = "proptest")]
use std::ops::Range;

/// A wrapper type which ensures a signed number is non-negative.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[repr(transparent)]
#[serde(transparent)]
pub struct NonNeg<T>(T)
where
    T: Signed + fmt::Display;

impl<T> NonNeg<T>
where
    T: Signed + fmt::Display,
{
    /// Returns the minimum value of the type.
    pub fn min() -> NonNeg<T> {
        NonNeg(T::zero())
    }

    /// Returns the maximum value of the type.
    pub fn max() -> NonNeg<T>
    where
        T: UpperBounded,
    {
        NonNeg(T::max_value())
    }

    /// Attempts to construct a `NonNeg` from its underlying type.
    ///
    /// Returns an error if `n` is negative.
    pub fn try_from(n: T) -> Result<NonNeg<T>, NonNegError> {
        match n.is_negative() {
            false => Ok(NonNeg(n)),
            true => Err(NonNegError),
        }
    }
}

impl<T> fmt::Display for NonNeg<T>
where
    T: Signed + fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T> Deref for NonNeg<T>
where
    T: Signed + fmt::Display,
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl From<NonNeg<i64>> for u64 {
    fn from(n: NonNeg<i64>) -> u64 {
        u64::try_from(*n).expect("non-negative")
    }
}

#[cfg(target_pointer_width = "64")]
impl crate::cast::CastFrom<NonNeg<i64>> for usize {
    #[allow(clippy::as_conversions)]
    fn cast_from(from: NonNeg<i64>) -> usize {
        usize::cast_from(u64::from(from))
    }
}

#[cfg(feature = "proptest")]
impl<T> Arbitrary for NonNeg<T>
where
    T: Signed + UpperBounded + fmt::Display + fmt::Debug + Copy + 'static,
    Range<T>: Strategy<Value = T>,
{
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (*Self::min()..*Self::max()).prop_map(NonNeg).boxed()
    }
}

/// An error indicating the attempted construction of a `NonNeg` with a negative
/// number.
#[derive(Debug, Clone)]
pub struct NonNegError;

impl fmt::Display for NonNegError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("cannot construct NonNeg from negative number")
    }
}

impl Error for NonNegError {}

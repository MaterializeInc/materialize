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

use std::fmt::{Display, Formatter};
use std::ops::{Add, Sub};

/// Overflowing number. Operations panic on overflow, even in release mode.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
pub struct Overflowing<T>(pub T);

impl<T: Display> Display for Overflowing<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Add<Self> for Overflowing<u32> {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        match self.0.overflowing_add(rhs.0) {
            (_, true) => panic!("Overflow {self} + {rhs}"),
            (result, false) => Self(result),
        }
    }
}

impl Sub<Self> for Overflowing<u32> {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        match self.0.overflowing_sub(rhs.0) {
            (_, true) => panic!("Overflow {self} - {rhs}"),
            (result, false) => Self(result),
        }
    }
}

impl TryFrom<usize> for Overflowing<u32> {
    type Error = <u32 as TryFrom<usize>>::Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        u32::try_from(value).map(Self)
    }
}

impl TryFrom<Overflowing<u32>> for usize {
    type Error = <usize as TryFrom<u32>>::Error;

    fn try_from(value: Overflowing<u32>) -> Result<Self, Self::Error> {
        Self::try_from(value.0)
    }
}

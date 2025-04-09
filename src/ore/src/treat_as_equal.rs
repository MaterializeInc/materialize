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

//! A newtype for values that should be ignored when comparing two values for equality.

use derivative::Derivative;
use serde::{Deserialize, Deserializer, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// Behaves like `T`, but has trivial `Hash`, `Eq`, `MzReflect`, and `Ord`
/// implementations. Does not appear in `Debug` output.
#[derive(Clone, Default, Derivative)]
#[derivative(Debug = "transparent")]
pub struct TreatAsEqual<T>(pub T);

impl<T> Hash for TreatAsEqual<T> {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}

impl<T> Eq for TreatAsEqual<T> {}

impl<T> PartialEq for TreatAsEqual<T> {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl<T> PartialOrd for TreatAsEqual<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for TreatAsEqual<T> {
    fn cmp(&self, _other: &Self) -> Ordering {
        Ordering::Equal
    }
}

impl<T: Serialize> Serialize for TreatAsEqual<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for TreatAsEqual<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(TreatAsEqual(T::deserialize(deserializer)?))
    }
}

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

//! Timestamp extensions for storing future updates in power-of-two buckets.

/// Timestamp extension for timestamps that can advance by `2^exponent`.
///
/// Most likely, this is only relevant for totally ordered timestamps.
pub trait BucketTimestamp: Sized {
    /// The number of bits in the timestamp.
    const DOMAIN: usize = size_of::<Self>() * 8;
    /// Advance this timestamp by `2^exponent`. Returns `None` if the
    /// timestamp would overflow.
    fn advance_by_power_of_two(&self, exponent: u32) -> Option<Self>;
}

impl BucketTimestamp for u8 {
    fn advance_by_power_of_two(&self, bits: u32) -> Option<Self> {
        self.checked_add(1_u8.checked_shl(bits)?)
    }
}

impl BucketTimestamp for u64 {
    fn advance_by_power_of_two(&self, bits: u32) -> Option<Self> {
        self.checked_add(1_u64.checked_shl(bits)?)
    }
}

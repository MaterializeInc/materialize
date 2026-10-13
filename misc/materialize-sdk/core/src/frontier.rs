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

//! The exclusive frontier, and the one place that converts it to an `AS OF`.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// An exclusive upper bound `F` on `mz_timestamp`: every update at a time
/// below `F` has been released, and none at or above it.
///
/// `F` ranges from 1 to 2^64. The upper end is reachable: a constant query
/// such as `SUBSCRIBE (SELECT 1)` emits its updates at the largest timestamp
/// (`test/sqllogictest/as_of.slt`), so the frontier that closes them is one
/// past any `u64`. The frontier is stored as `F - 1`, which is the `AS OF`
/// that resumes it.
///
/// In JSON a frontier is a decimal string, like every 64-bit integer in this
/// crate's JSON form.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Frontier {
    as_of: u64,
}

impl Frontier {
    /// Returns the frontier `frontier`, or `None` for zero, which no batch can
    /// close because no timestamp is below it.
    pub fn new(frontier: u64) -> Option<Frontier> {
        let as_of = frontier.checked_sub(1)?;
        Some(Frontier { as_of })
    }

    /// The frontier just past `time`, which closes `time` and everything
    /// before it.
    pub fn after(time: u64) -> Frontier {
        Frontier { as_of: time }
    }

    /// Whether `time` is below the frontier.
    pub fn contains(self, time: u64) -> bool {
        time <= self.as_of
    }

    /// The frontier as a number, which needs 65 bits.
    pub fn get(self) -> u128 {
        // A `u64` plus one cannot overflow a `u128`.
        u128::from(self.as_of).saturating_add(1)
    }

    /// The `AS OF` that resumes delivery at this frontier.
    ///
    /// With `SNAPSHOT = false`, `SUBSCRIBE ... AS OF t` emits updates at times
    /// strictly greater than `t` (`should_emit` in
    /// `src/compute/src/sink/subscribe.rs`), so `AS OF F - 1` emits exactly
    /// the updates at `F` and later.
    pub fn resume_as_of(self) -> u64 {
        self.as_of
    }
}

/// Prints `F`, never the stored `F - 1`.
impl fmt::Debug for Frontier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Frontier({})", self.get())
    }
}

impl fmt::Display for Frontier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(f)
    }
}

impl Serialize for Frontier {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Frontier {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        crate::wire::parse_u128(&s)
            .and_then(|f| f.checked_sub(1))
            .and_then(|as_of| u64::try_from(as_of).ok())
            .map(|as_of| Frontier { as_of })
            .ok_or_else(|| serde::de::Error::custom(format!("invalid frontier {s:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_is_not_a_frontier() {
        assert_eq!(Frontier::new(0), None);
    }

    #[test]
    fn resume_as_of_is_one_below_the_frontier() {
        let at = |t| Frontier::new(t).map(Frontier::resume_as_of);
        assert_eq!(at(1), Some(0));
        assert_eq!(at(1_700_000_000_001), Some(1_700_000_000_000));
        assert_eq!(Frontier::after(u64::MAX).resume_as_of(), u64::MAX);
    }

    #[test]
    fn the_frontier_after_the_largest_timestamp_exists() {
        let last = Frontier::after(u64::MAX);
        assert!(last.contains(u64::MAX));
        assert_eq!(last.get(), 1 << 64);
        assert!(last > Frontier::new(u64::MAX).unwrap());
        assert!(!Frontier::new(u64::MAX).unwrap().contains(u64::MAX));
    }

    #[test]
    fn debug_prints_the_frontier() {
        assert_eq!(format!("{:?}", Frontier::new(5).unwrap()), "Frontier(5)");
    }

    #[test]
    fn serializes_as_a_decimal_string() {
        for (frontier, json) in [
            (Frontier::new(1).unwrap(), r#""1""#),
            (Frontier::after(u64::MAX), r#""18446744073709551616""#),
        ] {
            assert_eq!(serde_json::to_string(&frontier).unwrap(), json);
            assert_eq!(serde_json::from_str::<Frontier>(json).unwrap(), frontier);
        }
        for invalid in [r#""0""#, r#""18446744073709551617""#, "1", r#""-1""#] {
            assert!(
                serde_json::from_str::<Frontier>(invalid).is_err(),
                "{invalid}"
            );
        }
    }
}

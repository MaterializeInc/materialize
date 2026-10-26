// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::str::FromStr;

use anyhow::{Error, anyhow};
#[cfg(any(test, feature = "proptest"))]
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

/// A shared adapter declaration's identity, independent of its name,
/// deployment, and execution.
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize
)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub struct AdapterId(pub u64);

impl fmt::Display for AdapterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a{}", self.0)
    }
}

impl FromStr for AdapterId {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .strip_prefix('a')
            .and_then(|id| id.parse().ok())
            .map(Self)
            .ok_or_else(|| anyhow!("invalid adapter ID: {value}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[mz_ore::test]
        fn text_roundtrip(id: AdapterId) {
            prop_assert_eq!(id.to_string().parse::<AdapterId>().unwrap(), id);
        }
    }

    #[mz_ore::test]
    fn rejects_other_id_namespaces() {
        for value in ["", "1", "u1", "s1", "a", "a-1", "a18446744073709551616"] {
            assert!(value.parse::<AdapterId>().is_err(), "{value}");
        }
    }
}

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
use mz_lowertest::MzReflect;
#[cfg(any(test, feature = "proptest"))]
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

/// The identifier for a query policy in the environment-wide namespace.
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
    Deserialize,
    MzReflect
)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub enum QueryPolicyId {
    System(u64),
    User(u64),
}

impl QueryPolicyId {
    pub fn is_system(&self) -> bool {
        matches!(self, Self::System(_))
    }

    pub fn is_user(&self) -> bool {
        matches!(self, Self::User(_))
    }

    pub fn is_builtin(&self) -> bool {
        self.is_system()
    }
}

impl FromStr for QueryPolicyId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid = || anyhow!("couldn't parse query policy id '{s}'");
        if let Some(id) = s.strip_prefix('s') {
            Ok(Self::System(id.parse().map_err(|_| invalid())?))
        } else if let Some(id) = s.strip_prefix('u') {
            Ok(Self::User(id.parse().map_err(|_| invalid())?))
        } else {
            Err(invalid())
        }
    }
}

impl fmt::Display for QueryPolicyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::System(id) => write!(f, "s{id}"),
            Self::User(id) => write!(f, "u{id}"),
        }
    }
}

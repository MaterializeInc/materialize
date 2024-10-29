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

use anyhow::{anyhow, Error};
use columnation::{Columnation, CopyRegion};
use mz_lowertest::MzReflect;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

const SYSTEM_CHAR: char = 's';
const USER_CHAR: char = 'u';

/// The identifier for a network policy.
#[derive(
    Arbitrary,
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
    MzReflect,
)]
pub enum NetworkPolicyId {
    System(u64),
    User(u64),
}

impl NetworkPolicyId {
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

impl FromStr for NetworkPolicyId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        fn parse_u64(s: &str) -> Result<u64, Error> {
            if s.len() < 2 {
                return Err(anyhow!("couldn't parse network policy id '{s}'"));
            }
            s[1..]
                .parse()
                .map_err(|_| anyhow!("couldn't parse network policy  id '{s}'"))
        }

        match s.chars().next() {
            Some(SYSTEM_CHAR) => {
                let val = parse_u64(s)?;
                Ok(Self::System(val))
            }
            Some(USER_CHAR) => {
                let val = parse_u64(s)?;
                Ok(Self::User(val))
            }
            _ => Err(anyhow!("couldn't parse network policy  id '{s}'")),
        }
    }
}

impl fmt::Display for NetworkPolicyId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::System(id) => write!(f, "{SYSTEM_CHAR}{id}"),
            Self::User(id) => write!(f, "{USER_CHAR}{id}"),
        }
    }
}

impl Columnation for NetworkPolicyId {
    type InnerRegion = CopyRegion<NetworkPolicyId>;
}

#[mz_ore::test]
fn test_network_policy_id_parsing() {
    let s = "s42";
    let network_policy_id: NetworkPolicyId = s.parse().unwrap();
    assert_eq!(NetworkPolicyId::System(42), network_policy_id);
    assert_eq!(s, network_policy_id.to_string());

    let s = "u666";
    let network_policy_id: NetworkPolicyId = s.parse().unwrap();
    assert_eq!(NetworkPolicyId::User(666), network_policy_id);
    assert_eq!(s, network_policy_id.to_string());

    let s = "d23";
    mz_ore::assert_err!(s.parse::<NetworkPolicyId>());

    let s = "asfje90uf23i";
    mz_ore::assert_err!(s.parse::<NetworkPolicyId>());

    let s = "";
    mz_ore::assert_err!(s.parse::<NetworkPolicyId>());
}

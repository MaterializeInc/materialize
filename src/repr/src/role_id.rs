// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::mem::size_of;
use std::str::FromStr;

use anyhow::{anyhow, Error};
use columnation::{CloneRegion, Columnation};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;

use mz_proto::{RustType, TryFromProtoError};

include!(concat!(env!("OUT_DIR"), "/mz_repr.role_id.rs"));

const SYSTEM_CHAR: char = 's';
const SYSTEM_BYTE: u8 = b's';
const USER_CHAR: char = 'u';
const USER_BYTE: u8 = b'u';
const PUBLIC_CHAR: char = 'p';
const PUBLIC_BYTE: u8 = b'p';

/// The identifier for a role.
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
pub enum RoleId {
    System(u64),
    User(u64),
    Public,
}

impl RoleId {
    pub fn is_system(&self) -> bool {
        matches!(self, Self::System(_))
    }

    pub fn is_user(&self) -> bool {
        matches!(self, Self::User(_))
    }

    pub fn is_public(&self) -> bool {
        matches!(self, Self::Public)
    }

    pub fn encode_binary(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(Self::binary_size());
        match self {
            RoleId::System(id) => {
                res.push(SYSTEM_BYTE);
                res.extend_from_slice(&id.to_le_bytes());
            }
            RoleId::User(id) => {
                res.push(USER_BYTE);
                res.extend_from_slice(&id.to_le_bytes());
            }
            RoleId::Public => {
                res.push(PUBLIC_BYTE);
                res.extend_from_slice(&0_u64.to_le_bytes());
            }
        }
        res
    }

    pub fn decode_binary(raw: &[u8]) -> Result<RoleId, Error> {
        if raw.len() != RoleId::binary_size() {
            return Err(anyhow!(
                "invalid binary size, expecting {}, found {}",
                RoleId::binary_size(),
                raw.len()
            ));
        }

        let variant = raw[0];
        let id = u64::from_le_bytes(raw[1..].try_into()?);

        match variant {
            SYSTEM_BYTE => Ok(RoleId::System(id)),
            USER_BYTE => Ok(RoleId::User(id)),
            PUBLIC_BYTE => Ok(RoleId::Public),
            _ => Err(anyhow!("unrecognized role id variant byte '{variant}'")),
        }
    }

    pub const fn binary_size() -> usize {
        1 + size_of::<u64>()
    }
}

impl FromStr for RoleId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.chars().next() {
            Some(SYSTEM_CHAR) => {
                if s.len() < 2 {
                    return Err(anyhow!("couldn't parse role id {}", s));
                }
                let val: u64 = s[1..].parse()?;
                Ok(Self::System(val))
            }
            Some(USER_CHAR) => {
                if s.len() < 2 {
                    return Err(anyhow!("couldn't parse role id {}", s));
                }
                let val: u64 = s[1..].parse()?;
                Ok(Self::User(val))
            }
            Some(PUBLIC_CHAR) => {
                if s.len() == 1 {
                    Ok(Self::Public)
                } else {
                    Err(anyhow!("couldn't parse role id {s}"))
                }
            }
            _ => Err(anyhow!("couldn't parse role id {s}")),
        }
    }
}

impl fmt::Display for RoleId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::System(id) => write!(f, "{SYSTEM_CHAR}{id}"),
            Self::User(id) => write!(f, "{USER_CHAR}{id}"),
            Self::Public => write!(f, "{PUBLIC_CHAR}"),
        }
    }
}

impl RustType<ProtoRoleId> for RoleId {
    fn into_proto(&self) -> ProtoRoleId {
        use proto_role_id::Kind::*;
        ProtoRoleId {
            kind: Some(match self {
                RoleId::System(x) => System(*x),
                RoleId::User(x) => User(*x),
                RoleId::Public => Public(()),
            }),
        }
    }

    fn from_proto(proto: ProtoRoleId) -> Result<Self, TryFromProtoError> {
        use proto_role_id::Kind::*;
        match proto.kind {
            Some(System(x)) => Ok(RoleId::System(x)),
            Some(User(x)) => Ok(RoleId::User(x)),
            Some(Public(_)) => Ok(RoleId::Public),
            None => Err(TryFromProtoError::missing_field("ProtoRoleId::kind")),
        }
    }
}

impl Columnation for RoleId {
    type InnerRegion = CloneRegion<RoleId>;
}

#[test]
fn test_role_id_parsing() {
    let s = "s42";
    let role_id: RoleId = s.parse().unwrap();
    assert_eq!(RoleId::System(42), role_id);
    assert_eq!(s, role_id.to_string());

    let s = "u666";
    let role_id: RoleId = s.parse().unwrap();
    assert_eq!(RoleId::User(666), role_id);
    assert_eq!(s, role_id.to_string());

    let s = "p";
    let role_id: RoleId = s.parse().unwrap();
    assert_eq!(RoleId::Public, role_id);
    assert_eq!(s, role_id.to_string());

    let s = "p23";
    assert!(s.parse::<RoleId>().is_err());

    let s = "d23";
    assert!(s.parse::<RoleId>().is_err());

    let s = "asfje90uf23i";
    assert!(s.parse::<RoleId>().is_err());

    let s = "";
    assert!(s.parse::<RoleId>().is_err());
}

#[test]
fn test_role_id_binary() {
    let role_id = RoleId::System(42);
    assert_eq!(
        role_id,
        RoleId::decode_binary(&role_id.encode_binary()).unwrap()
    );

    let role_id = RoleId::User(666);
    assert_eq!(
        role_id,
        RoleId::decode_binary(&role_id.encode_binary()).unwrap()
    );

    let role_id = RoleId::Public;
    assert_eq!(
        role_id,
        RoleId::decode_binary(&role_id.encode_binary()).unwrap()
    );

    assert!(RoleId::decode_binary(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 0]).is_err())
}

#[test]
fn test_role_id_binary_size() {
    assert_eq!(9, RoleId::binary_size());
}

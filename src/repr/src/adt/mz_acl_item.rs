// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Materialize specific access control list abstract data type.

use crate::role_id::RoleId;
use anyhow::{anyhow, Error};
use bitflags::bitflags;
use columnation::{CloneRegion, Columnation};
use mz_proto::{RustType, TryFromProtoError};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::mem::size_of;
use std::ops::BitOrAssign;
use std::str::FromStr;

include!(concat!(env!("OUT_DIR"), "/mz_repr.adt.mz_acl_item.rs"));

const INSERT_CHAR: char = 'a';
const SELECT_CHAR: char = 'r';
const UPDATE_CHAR: char = 'w';
const DELETE_CHAR: char = 'd';
const USAGE_CHAR: char = 'U';
const CREATE_CHAR: char = 'C';

bitflags! {
    /// A bit flag representing all the privileges that can be granted to a role.
    ///
    /// Modeled after:
    /// https://github.com/postgres/postgres/blob/7f5b19817eaf38e70ad1153db4e644ee9456853e/src/include/nodes/parsenodes.h#L74-L101
    #[derive(Serialize, Deserialize)]
    pub struct AclMode: u64 {
        const INSERT = 1 << 0;
        const SELECT = 1 << 1;
        const UPDATE = 1 << 2;
        const DELETE = 1 << 3;
        const USAGE = 1 << 8;
        const CREATE = 1 << 9;
    }
}

impl FromStr for AclMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut acl_mode = AclMode::empty();
        for c in s.chars() {
            match c {
                INSERT_CHAR => acl_mode.bitor_assign(AclMode::INSERT),
                SELECT_CHAR => acl_mode.bitor_assign(AclMode::SELECT),
                UPDATE_CHAR => acl_mode.bitor_assign(AclMode::UPDATE),
                DELETE_CHAR => acl_mode.bitor_assign(AclMode::DELETE),
                USAGE_CHAR => acl_mode.bitor_assign(AclMode::USAGE),
                CREATE_CHAR => acl_mode.bitor_assign(AclMode::CREATE),
                _ => return Err(anyhow!("invalid privilege '{c}' in acl mode '{s}'")),
            }
        }
        Ok(acl_mode)
    }
}

impl fmt::Display for AclMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // The order of each flag matches PostgreSQL, which uses the same order that they're
        // defined in.
        if self.contains(AclMode::INSERT) {
            write!(f, "{INSERT_CHAR}")?;
        }
        if self.contains(AclMode::SELECT) {
            write!(f, "{SELECT_CHAR}")?;
        }
        if self.contains(AclMode::UPDATE) {
            write!(f, "{UPDATE_CHAR}")?;
        }
        if self.contains(AclMode::DELETE) {
            write!(f, "{DELETE_CHAR}")?;
        }
        if self.contains(AclMode::USAGE) {
            write!(f, "{USAGE_CHAR}")?;
        }
        if self.contains(AclMode::CREATE) {
            write!(f, "{CREATE_CHAR}")?;
        }
        Ok(())
    }
}

impl RustType<ProtoAclMode> for AclMode {
    fn into_proto(&self) -> ProtoAclMode {
        ProtoAclMode {
            acl_mode: self.bits,
        }
    }

    fn from_proto(proto: ProtoAclMode) -> Result<Self, TryFromProtoError> {
        Ok(AclMode {
            bits: proto.acl_mode,
        })
    }
}
impl Columnation for AclMode {
    type InnerRegion = CloneRegion<AclMode>;
}

/// A list of privileges granted to a role in Materialize.
///
/// This is modelled after the AclItem type in PostgreSQL, but the OIDs are replaced with RoleIds
/// because we don't use OID as persistent identifiers for roles.
///
/// See: <https://github.com/postgres/postgres/blob/7f5b19817eaf38e70ad1153db4e644ee9456853e/src/include/utils/acl.h#L48-L59>
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Hash, Deserialize)]
pub struct MzAclItem {
    /// Role that this item grants privileges to.
    pub grantee: RoleId,
    /// Grantor of privileges.
    pub grantor: RoleId,
    /// Privileges bit flag.
    pub acl_mode: AclMode,
}

impl MzAclItem {
    pub fn encode_binary(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(Self::binary_size());
        res.extend_from_slice(&self.grantee.encode_binary());
        res.extend_from_slice(&self.grantor.encode_binary());
        res.extend_from_slice(&self.acl_mode.bits().to_le_bytes());
        res
    }

    pub fn decode_binary(raw: &[u8]) -> Result<MzAclItem, Error> {
        if raw.len() != MzAclItem::binary_size() {
            return Err(anyhow!(
                "invalid binary size, expecting {}, found {}",
                MzAclItem::binary_size(),
                raw.len()
            ));
        }

        let role_id_size = RoleId::binary_size();

        let grantee = RoleId::decode_binary(&raw[0..role_id_size])?;
        let raw = &raw[role_id_size..];
        let grantor = RoleId::decode_binary(&raw[0..role_id_size])?;
        let raw = &raw[role_id_size..];
        let acl_mode = u64::from_le_bytes(raw.try_into()?);

        Ok(MzAclItem {
            grantee,
            grantor,
            acl_mode: AclMode { bits: acl_mode },
        })
    }

    pub const fn binary_size() -> usize {
        RoleId::binary_size() + RoleId::binary_size() + size_of::<u64>()
    }
}

impl FromStr for MzAclItem {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<_> = s.split(&['=', '/']).collect();
        if parts.len() != 3 {
            return Err(anyhow!("invalid mz_aclitem '{s}'"));
        }
        let grantee: RoleId = if parts[0].is_empty() {
            RoleId::Public
        } else {
            parts[0].parse()?
        };
        let acl_mode: AclMode = parts[1].parse()?;
        let grantor: RoleId = parts[2].parse()?;

        Ok(MzAclItem {
            grantee,
            grantor,
            acl_mode,
        })
    }
}

impl fmt::Display for MzAclItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if !self.grantee.is_public() {
            write!(f, "{}", self.grantee)?;
        }
        write!(f, "={}/{}", self.acl_mode, self.grantor)
    }
}

impl RustType<ProtoMzAclItem> for MzAclItem {
    fn into_proto(&self) -> ProtoMzAclItem {
        ProtoMzAclItem {
            grantee: Some(self.grantee.into_proto()),
            grantor: Some(self.grantor.into_proto()),
            acl_mode: Some(self.acl_mode.into_proto()),
        }
    }

    fn from_proto(proto: ProtoMzAclItem) -> Result<Self, TryFromProtoError> {
        match (proto.grantee, proto.grantor, proto.acl_mode) {
            (Some(grantee), Some(grantor), Some(acl_mode)) => Ok(MzAclItem {
                grantee: RoleId::from_proto(grantee)?,
                grantor: RoleId::from_proto(grantor)?,
                acl_mode: AclMode::from_proto(acl_mode)?,
            }),
            (None, _, _) => Err(TryFromProtoError::missing_field("ProtoMzAclItem::grantee")),
            (_, None, _) => Err(TryFromProtoError::missing_field("ProtoMzAclItem::grantor")),
            (_, _, None) => Err(TryFromProtoError::missing_field("ProtoMzAclItem::acl_mode")),
        }
    }
}

impl Columnation for MzAclItem {
    type InnerRegion = CloneRegion<MzAclItem>;
}

#[test]
fn test_mz_acl_parsing() {
    let s = "u42=rw/s666";
    let mz_acl: MzAclItem = s.parse().unwrap();
    assert_eq!(RoleId::User(42), mz_acl.grantee);
    assert_eq!(RoleId::System(666), mz_acl.grantor);
    assert!(!mz_acl.acl_mode.contains(AclMode::INSERT));
    assert!(mz_acl.acl_mode.contains(AclMode::SELECT));
    assert!(mz_acl.acl_mode.contains(AclMode::UPDATE));
    assert!(!mz_acl.acl_mode.contains(AclMode::DELETE));
    assert!(!mz_acl.acl_mode.contains(AclMode::USAGE));
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE));
    assert_eq!(s, mz_acl.to_string());

    let s = "=UC/u4";
    let mz_acl: MzAclItem = s.parse().unwrap();
    assert_eq!(RoleId::Public, mz_acl.grantee);
    assert_eq!(RoleId::User(4), mz_acl.grantor);
    assert!(!mz_acl.acl_mode.contains(AclMode::INSERT));
    assert!(!mz_acl.acl_mode.contains(AclMode::SELECT));
    assert!(!mz_acl.acl_mode.contains(AclMode::UPDATE));
    assert!(!mz_acl.acl_mode.contains(AclMode::DELETE));
    assert!(mz_acl.acl_mode.contains(AclMode::USAGE));
    assert!(mz_acl.acl_mode.contains(AclMode::CREATE));
    assert_eq!(s, mz_acl.to_string());

    let s = "s7=/s12";
    let mz_acl: MzAclItem = s.parse().unwrap();
    assert_eq!(RoleId::System(7), mz_acl.grantee);
    assert_eq!(RoleId::System(12), mz_acl.grantor);
    assert!(!mz_acl.acl_mode.contains(AclMode::INSERT));
    assert!(!mz_acl.acl_mode.contains(AclMode::SELECT));
    assert!(!mz_acl.acl_mode.contains(AclMode::UPDATE));
    assert!(!mz_acl.acl_mode.contains(AclMode::DELETE));
    assert!(!mz_acl.acl_mode.contains(AclMode::USAGE));
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE));
    assert_eq!(s, mz_acl.to_string());

    let s = "=/u100";
    let mz_acl: MzAclItem = s.parse().unwrap();
    assert_eq!(RoleId::Public, mz_acl.grantee);
    assert_eq!(RoleId::User(100), mz_acl.grantor);
    assert!(!mz_acl.acl_mode.contains(AclMode::INSERT));
    assert!(!mz_acl.acl_mode.contains(AclMode::SELECT));
    assert!(!mz_acl.acl_mode.contains(AclMode::UPDATE));
    assert!(!mz_acl.acl_mode.contains(AclMode::DELETE));
    assert!(!mz_acl.acl_mode.contains(AclMode::USAGE));
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE));
    assert_eq!(s, mz_acl.to_string());

    assert!("u32=C/".parse::<MzAclItem>().is_err());
    assert!("=/".parse::<MzAclItem>().is_err());
    assert!("f62hfiuew827fhh".parse::<MzAclItem>().is_err());
    assert!("u2=rw/s66=CU/u33".parse::<MzAclItem>().is_err());
}

#[test]
fn test_mz_acl_item_binary() {
    use std::ops::BitAnd;

    let mz_acl_item = MzAclItem {
        grantee: RoleId::User(42),
        grantor: RoleId::System(666),
        acl_mode: AclMode::empty()
            .bitand(AclMode::SELECT)
            .bitand(AclMode::UPDATE),
    };
    assert_eq!(
        mz_acl_item,
        MzAclItem::decode_binary(&mz_acl_item.encode_binary()).unwrap()
    );

    let mz_acl_item = MzAclItem {
        grantee: RoleId::Public,
        grantor: RoleId::User(4),
        acl_mode: AclMode::empty()
            .bitand(AclMode::USAGE)
            .bitand(AclMode::CREATE),
    };
    assert_eq!(
        mz_acl_item,
        MzAclItem::decode_binary(&mz_acl_item.encode_binary()).unwrap()
    );

    let mz_acl_item = MzAclItem {
        grantee: RoleId::System(7),
        grantor: RoleId::System(12),
        acl_mode: AclMode::empty(),
    };
    assert_eq!(
        mz_acl_item,
        MzAclItem::decode_binary(&mz_acl_item.encode_binary()).unwrap()
    );

    let mz_acl_item = MzAclItem {
        grantee: RoleId::Public,
        grantor: RoleId::User(100),
        acl_mode: AclMode::empty(),
    };
    assert_eq!(
        mz_acl_item,
        MzAclItem::decode_binary(&mz_acl_item.encode_binary()).unwrap()
    );

    assert!(MzAclItem::decode_binary(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 0]).is_err())
}

#[test]
fn test_mz_acl_item_binary_size() {
    assert_eq!(26, MzAclItem::binary_size());
}

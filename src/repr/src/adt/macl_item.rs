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
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::mem::size_of;
use std::ops::BitOrAssign;
use std::str::FromStr;

include!(concat!(env!("OUT_DIR"), "/mz_repr.adt.macl_item.rs"));

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
        const SELECT = 1 << 1 ;
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
/// because we don't use OID as persistent identifiers for roles. The 'm' in 'maclitem' stands
/// for Materialize.
///
/// See: <https://github.com/postgres/postgres/blob/7f5b19817eaf38e70ad1153db4e644ee9456853e/src/include/utils/acl.h#L48-L59>
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Hash, Deserialize)]
pub struct MaclItem {
    /// Role that this item grants privileges to.
    pub grantee: RoleId,
    /// Grantor of privileges.
    pub grantor: RoleId,
    /// Privileges bit flag.
    pub acl_mode: AclMode,
}

impl MaclItem {
    pub fn encode_binary(&self) -> Vec<u8> {
        let mut res = Vec::new();
        res.extend_from_slice(&self.grantee.encode_binary());
        res.extend_from_slice(&self.grantor.encode_binary());
        res.extend_from_slice(&self.acl_mode.bits().to_le_bytes());
        res
    }

    pub fn decode_binary(raw: &[u8]) -> Result<MaclItem, Error> {
        if raw.len() != MaclItem::binary_size() {
            return Err(anyhow!(
                "invalid binary size, expecting {}, found {}",
                MaclItem::binary_size(),
                raw.len()
            ));
        }

        let role_id_size = RoleId::binary_size();

        let grantee = RoleId::decode_binary(&raw[0..role_id_size])?;
        let raw = &raw[role_id_size..];
        let grantor = RoleId::decode_binary(&raw[0..role_id_size])?;
        let raw = &raw[role_id_size..];
        let acl_mode = u64::from_le_bytes(raw.try_into()?);

        Ok(MaclItem {
            grantee,
            grantor,
            acl_mode: AclMode { bits: acl_mode },
        })
    }

    pub const fn binary_size() -> usize {
        RoleId::binary_size() + RoleId::binary_size() + size_of::<AclMode>()
    }
}

impl FromStr for MaclItem {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let re: Regex = Regex::new(&format!(
            r"(.*)=([{INSERT_CHAR},{SELECT_CHAR},{UPDATE_CHAR},{DELETE_CHAR},{USAGE_CHAR},{CREATE_CHAR}]*)/(.+)"
        ))?;
        let caps = re
            .captures(s)
            .ok_or_else(|| anyhow!("invalid maclitem '{s}'"))?;

        let grantee: &str = caps.get(1).map_or("", |m| m.as_str());
        let grantee = if grantee.is_empty() {
            RoleId::Public
        } else {
            grantee.parse()?
        };
        let acl_mode: AclMode = caps
            .get(2)
            .ok_or_else(|| anyhow!("invalid maclitem '{s}'"))?
            .as_str()
            .parse()?;
        let grantor: RoleId = caps
            .get(3)
            .ok_or_else(|| anyhow!("invalid maclitem '{s}'"))?
            .as_str()
            .parse()?;

        Ok(MaclItem {
            grantee,
            grantor,
            acl_mode,
        })
    }
}

impl fmt::Display for MaclItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if !self.grantee.is_public() {
            write!(f, "{}", self.grantee)?;
        }
        write!(f, "={}/{}", self.acl_mode, self.grantor)
    }
}

impl RustType<ProtoMaclItem> for MaclItem {
    fn into_proto(&self) -> ProtoMaclItem {
        ProtoMaclItem {
            grantee: Some(self.grantee.into_proto()),
            grantor: Some(self.grantor.into_proto()),
            acl_mode: Some(self.acl_mode.into_proto()),
        }
    }

    fn from_proto(proto: ProtoMaclItem) -> Result<Self, TryFromProtoError> {
        match (proto.grantee, proto.grantor, proto.acl_mode) {
            (Some(grantee), Some(grantor), Some(acl_mode)) => Ok(MaclItem {
                grantee: RoleId::from_proto(grantee)?,
                grantor: RoleId::from_proto(grantor)?,
                acl_mode: AclMode::from_proto(acl_mode)?,
            }),
            (None, _, _) => Err(TryFromProtoError::missing_field("ProtoMaclItem::grantee")),
            (_, None, _) => Err(TryFromProtoError::missing_field("ProtoMaclItem::grantor")),
            (_, _, None) => Err(TryFromProtoError::missing_field("ProtoMaclItem::acl_mode")),
        }
    }
}

impl Columnation for MaclItem {
    type InnerRegion = CloneRegion<MaclItem>;
}

#[test]
fn test_macl_parsing() {
    let s = "u42=rw/s666";
    let macl: MaclItem = s.parse().unwrap();
    assert_eq!(RoleId::User(42), macl.grantee);
    assert_eq!(RoleId::System(666), macl.grantor);
    assert!(!macl.acl_mode.contains(AclMode::INSERT));
    assert!(macl.acl_mode.contains(AclMode::SELECT));
    assert!(macl.acl_mode.contains(AclMode::UPDATE));
    assert!(!macl.acl_mode.contains(AclMode::DELETE));
    assert!(!macl.acl_mode.contains(AclMode::USAGE));
    assert!(!macl.acl_mode.contains(AclMode::CREATE));
    assert_eq!(s, macl.to_string());

    let s = "=UC/u4";
    let macl: MaclItem = s.parse().unwrap();
    assert_eq!(RoleId::Public, macl.grantee);
    assert_eq!(RoleId::User(4), macl.grantor);
    assert!(!macl.acl_mode.contains(AclMode::INSERT));
    assert!(!macl.acl_mode.contains(AclMode::SELECT));
    assert!(!macl.acl_mode.contains(AclMode::UPDATE));
    assert!(!macl.acl_mode.contains(AclMode::DELETE));
    assert!(macl.acl_mode.contains(AclMode::USAGE));
    assert!(macl.acl_mode.contains(AclMode::CREATE));
    assert_eq!(s, macl.to_string());

    let s = "s7=/s12";
    let macl: MaclItem = s.parse().unwrap();
    assert_eq!(RoleId::System(7), macl.grantee);
    assert_eq!(RoleId::System(12), macl.grantor);
    assert!(!macl.acl_mode.contains(AclMode::INSERT));
    assert!(!macl.acl_mode.contains(AclMode::SELECT));
    assert!(!macl.acl_mode.contains(AclMode::UPDATE));
    assert!(!macl.acl_mode.contains(AclMode::DELETE));
    assert!(!macl.acl_mode.contains(AclMode::USAGE));
    assert!(!macl.acl_mode.contains(AclMode::CREATE));
    assert_eq!(s, macl.to_string());

    let s = "=/u100";
    let macl: MaclItem = s.parse().unwrap();
    assert_eq!(RoleId::Public, macl.grantee);
    assert_eq!(RoleId::User(100), macl.grantor);
    assert!(!macl.acl_mode.contains(AclMode::INSERT));
    assert!(!macl.acl_mode.contains(AclMode::SELECT));
    assert!(!macl.acl_mode.contains(AclMode::UPDATE));
    assert!(!macl.acl_mode.contains(AclMode::DELETE));
    assert!(!macl.acl_mode.contains(AclMode::USAGE));
    assert!(!macl.acl_mode.contains(AclMode::CREATE));
    assert_eq!(s, macl.to_string());

    assert!("u32=C/".parse::<MaclItem>().is_err());
    assert!("=/".parse::<MaclItem>().is_err());
    assert!("f62hfiuew827fhh".parse::<MaclItem>().is_err());
}

#[test]
fn test_macl_item_binary() {
    use std::ops::BitAnd;

    let macl_item = MaclItem {
        grantee: RoleId::User(42),
        grantor: RoleId::System(666),
        acl_mode: AclMode::empty()
            .bitand(AclMode::SELECT)
            .bitand(AclMode::UPDATE),
    };
    assert_eq!(
        macl_item,
        MaclItem::decode_binary(&macl_item.encode_binary()).unwrap()
    );

    let macl_item = MaclItem {
        grantee: RoleId::Public,
        grantor: RoleId::User(4),
        acl_mode: AclMode::empty()
            .bitand(AclMode::USAGE)
            .bitand(AclMode::CREATE),
    };
    assert_eq!(
        macl_item,
        MaclItem::decode_binary(&macl_item.encode_binary()).unwrap()
    );

    let macl_item = MaclItem {
        grantee: RoleId::System(7),
        grantor: RoleId::System(12),
        acl_mode: AclMode::empty(),
    };
    assert_eq!(
        macl_item,
        MaclItem::decode_binary(&macl_item.encode_binary()).unwrap()
    );

    let macl_item = MaclItem {
        grantee: RoleId::Public,
        grantor: RoleId::User(100),
        acl_mode: AclMode::empty(),
    };
    assert_eq!(
        macl_item,
        MaclItem::decode_binary(&macl_item.encode_binary()).unwrap()
    );

    assert!(MaclItem::decode_binary(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 0]).is_err())
}

#[test]
fn test_macl_item_binary_size() {
    assert_eq!(26, MaclItem::binary_size());
}

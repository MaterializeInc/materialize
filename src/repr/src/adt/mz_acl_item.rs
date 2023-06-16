// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Materialize specific access control list abstract data type.

use std::collections::BTreeMap;
use std::fmt;
use std::mem::size_of;
use std::ops::BitOrAssign;
use std::str::FromStr;

use anyhow::{anyhow, Error};
use bitflags::bitflags;
use columnation::{CloneRegion, Columnation};
use mz_ore::str::StrExt;
use mz_proto::{RustType, TryFromProtoError};
use proptest::arbitrary::Arbitrary;
use proptest::strategy::{BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::role_id::RoleId;

include!(concat!(env!("OUT_DIR"), "/mz_repr.adt.mz_acl_item.rs"));

// Append
const INSERT_CHAR: char = 'a';
// Read
const SELECT_CHAR: char = 'r';
// Write
const UPDATE_CHAR: char = 'w';
// Delete
const DELETE_CHAR: char = 'd';
// Usage
const USAGE_CHAR: char = 'U';
// Create
const CREATE_CHAR: char = 'C';
// Role
const CREATE_ROLE_CHAR: char = 'R';
// dataBase
const CREATE_DB_CHAR: char = 'B';
// compute Node
const CREATE_CLUSTER_CHAR: char = 'N';

const INSERT_STR: &str = "INSERT";
const SELECT_STR: &str = "SELECT";
const UPDATE_STR: &str = "UPDATE";
const DELETE_STR: &str = "DELETE";
const USAGE_STR: &str = "USAGE";
const CREATE_STR: &str = "CREATE";
const CREATE_ROLE_STR: &str = "CREATEROLE";
const CREATE_DB_STR: &str = "CREATEDB";
const CREATE_CLUSTER_STR: &str = "CREATECLUSTER";

bitflags! {
    /// A bit flag representing all the privileges that can be granted to a role.
    ///
    /// Modeled after:
    /// https://github.com/postgres/postgres/blob/7f5b19817eaf38e70ad1153db4e644ee9456853e/src/include/nodes/parsenodes.h#L74-L101
    ///
    /// The lower 32 bits are used for different privilege types.
    ///
    /// The upper 32 bits indicate a grant option on the privilege for the current bit shifted
    /// right by 32 bits (Currently unimplemented in Materialize).
    ///
    /// Privileges that exist in Materialize but not PostgreSQL start at the highest available bit
    /// and move down towards the PostgreSQL compatible bits. This is try to avoid collisions with
    /// privileges that PostgreSQL may add in the future.
    #[derive(Serialize, Deserialize)]
    pub struct AclMode: u64 {
        // PostgreSQL compatible privileges.
        const INSERT = 1 << 0;
        const SELECT = 1 << 1;
        const UPDATE = 1 << 2;
        const DELETE = 1 << 3;
        const USAGE = 1 << 8;
        const CREATE = 1 << 9;

        // Materialize custom privileges.
        const CREATE_CLUSTER = 1 << 29;
        const CREATE_DB = 1 << 30;
        const CREATE_ROLE = 1 << 31;

        // No additional privileges should be defined at a bit larger than 1 << 31. Those bits are
        // reserved for grant options.
    }
}

impl AclMode {
    pub fn parse_single_privilege(s: &str) -> Result<Self, Error> {
        match s.trim().to_uppercase().as_str() {
            INSERT_STR => Ok(AclMode::INSERT),
            SELECT_STR => Ok(AclMode::SELECT),
            UPDATE_STR => Ok(AclMode::UPDATE),
            DELETE_STR => Ok(AclMode::DELETE),
            USAGE_STR => Ok(AclMode::USAGE),
            CREATE_STR => Ok(AclMode::CREATE),
            CREATE_ROLE_STR => Ok(AclMode::CREATE_ROLE),
            CREATE_DB_STR => Ok(AclMode::CREATE_DB),
            CREATE_CLUSTER_STR => Ok(AclMode::CREATE_CLUSTER),
            _ => Err(anyhow!("unrecognized privilege type: {}", s.quoted())),
        }
    }

    pub fn parse_multiple_privileges(s: &str) -> Result<Self, Error> {
        let mut acl_mode = AclMode::empty();
        for privilege in s.split(',') {
            let privilege = AclMode::parse_single_privilege(privilege)?;
            acl_mode.bitor_assign(privilege);
        }
        Ok(acl_mode)
    }

    pub fn to_error_string(&self) -> String {
        let mut privileges = Vec::new();
        if self.contains(AclMode::SELECT) {
            privileges.push(SELECT_STR);
        }
        if self.contains(AclMode::INSERT) {
            privileges.push(INSERT_STR);
        }
        if self.contains(AclMode::UPDATE) {
            privileges.push(UPDATE_STR);
        }
        if self.contains(AclMode::DELETE) {
            privileges.push(DELETE_STR);
        }
        if self.contains(AclMode::USAGE) {
            privileges.push(USAGE_STR);
        }
        if self.contains(AclMode::CREATE) {
            privileges.push(CREATE_STR);
        }
        if self.contains(AclMode::CREATE_ROLE) {
            privileges.push(CREATE_ROLE_STR);
        }
        if self.contains(AclMode::CREATE_DB) {
            privileges.push(CREATE_DB_STR);
        }
        if self.contains(AclMode::CREATE_CLUSTER) {
            privileges.push(CREATE_CLUSTER_STR);
        }
        privileges.join(", ")
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
                CREATE_ROLE_CHAR => acl_mode.bitor_assign(AclMode::CREATE_ROLE),
                CREATE_DB_CHAR => acl_mode.bitor_assign(AclMode::CREATE_DB),
                CREATE_CLUSTER_CHAR => acl_mode.bitor_assign(AclMode::CREATE_CLUSTER),
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
        if self.contains(AclMode::CREATE_ROLE) {
            write!(f, "{CREATE_ROLE_CHAR}")?;
        }
        if self.contains(AclMode::CREATE_DB) {
            write!(f, "{CREATE_DB_CHAR}")?;
        }
        if self.contains(AclMode::CREATE_CLUSTER) {
            write!(f, "{CREATE_CLUSTER_CHAR}")?;
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

impl Arbitrary for AclMode {
    type Parameters = ();
    type Strategy = BoxedStrategy<AclMode>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        proptest::bits::BitSetStrategy::masked(AclMode::all().bits)
            .prop_map(|bits| AclMode::from_bits(bits).expect("invalid proptest implementation"))
            .boxed()
    }
}

/// A list of privileges granted to a role in Materialize.
///
/// This is modelled after the AclItem type in PostgreSQL, but the OIDs are replaced with RoleIds
/// because we don't use OID as persistent identifiers for roles.
///
/// See: <https://github.com/postgres/postgres/blob/7f5b19817eaf38e70ad1153db4e644ee9456853e/src/include/utils/acl.h#L48-L59>
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Hash, Deserialize, Arbitrary,
)]
pub struct MzAclItem {
    /// Role that this item grants privileges to.
    pub grantee: RoleId,
    /// Grantor of privileges.
    pub grantor: RoleId,
    /// Privileges bit flag.
    pub acl_mode: AclMode,
}

impl MzAclItem {
    pub fn empty(grantee: RoleId, grantor: RoleId) -> MzAclItem {
        MzAclItem {
            grantee,
            grantor,
            acl_mode: AclMode::empty(),
        }
    }

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

    pub fn group_by_grantee(items: Vec<MzAclItem>) -> PrivilegeMap {
        PrivilegeMap::new(
            items
                .into_iter()
                .fold(BTreeMap::new(), |mut accum, mz_acl_item| {
                    accum
                        .entry(mz_acl_item.grantee)
                        .or_default()
                        .push(mz_acl_item);
                    accum
                }),
        )
    }

    pub fn flatten(items: &PrivilegeMap) -> Vec<MzAclItem> {
        items
            .0
            .values()
            .map(|items| items.into_iter())
            .flatten()
            .cloned()
            .collect()
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

/// Key is the role that granted the privilege, value is the privilege itself.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct PrivilegeMap(pub BTreeMap<RoleId, Vec<MzAclItem>>);

impl PrivilegeMap {
    pub fn new(privilege_map: BTreeMap<RoleId, Vec<MzAclItem>>) -> PrivilegeMap {
        PrivilegeMap(privilege_map)
    }

    pub fn all_values(&self) -> impl Iterator<Item = &MzAclItem> {
        self.0
            .values()
            .flat_map(|privileges| privileges.into_iter())
    }
}

impl Default for PrivilegeMap {
    fn default() -> PrivilegeMap {
        PrivilegeMap::new(BTreeMap::new())
    }
}

/// Combines all [`MzAclItem`]s that have the same grantee and grantor.
pub fn merge_mz_acl_items(
    mz_acl_items: impl Iterator<Item = MzAclItem>,
) -> impl Iterator<Item = MzAclItem> {
    mz_acl_items
        .fold(BTreeMap::new(), |mut accum, mz_acl_item| {
            let item = accum
                .entry((mz_acl_item.grantee, mz_acl_item.grantor))
                .or_insert(MzAclItem::empty(mz_acl_item.grantee, mz_acl_item.grantor));
            item.acl_mode |= mz_acl_item.acl_mode;
            accum
        })
        .into_values()
}

#[mz_ore::test]
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
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_ROLE));
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_DB));
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_CLUSTER));
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
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_ROLE));
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_DB));
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_CLUSTER));
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
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_ROLE));
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_DB));
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_CLUSTER));
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
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_ROLE));
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_DB));
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_CLUSTER));
    assert_eq!(s, mz_acl.to_string());

    let s = "u1=RBN/u2";
    let mz_acl: MzAclItem = s.parse().unwrap();
    assert_eq!(RoleId::User(1), mz_acl.grantee);
    assert_eq!(RoleId::User(2), mz_acl.grantor);
    assert!(!mz_acl.acl_mode.contains(AclMode::INSERT));
    assert!(!mz_acl.acl_mode.contains(AclMode::SELECT));
    assert!(!mz_acl.acl_mode.contains(AclMode::UPDATE));
    assert!(!mz_acl.acl_mode.contains(AclMode::DELETE));
    assert!(!mz_acl.acl_mode.contains(AclMode::USAGE));
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE));
    assert!(mz_acl.acl_mode.contains(AclMode::CREATE_ROLE));
    assert!(mz_acl.acl_mode.contains(AclMode::CREATE_DB));
    assert!(mz_acl.acl_mode.contains(AclMode::CREATE_CLUSTER));
    assert_eq!(s, mz_acl.to_string());

    assert!("u32=C/".parse::<MzAclItem>().is_err());
    assert!("=/".parse::<MzAclItem>().is_err());
    assert!("f62hfiuew827fhh".parse::<MzAclItem>().is_err());
    assert!("u2=rw/s66=CU/u33".parse::<MzAclItem>().is_err());
}

#[mz_ore::test]
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

#[mz_ore::test]
fn test_mz_acl_item_binary_size() {
    assert_eq!(26, MzAclItem::binary_size());
}

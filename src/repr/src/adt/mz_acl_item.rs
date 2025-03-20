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

use crate::adt::system::Oid;
use anyhow::{anyhow, Error};
use bitflags::bitflags;
use columnation::{Columnation, CopyRegion};
use mz_ore::soft_assert_no_log;
use mz_ore::str::StrExt;
use mz_persist_types::columnar::FixedSizeCodec;
use mz_proto::{RustType, TryFromProtoError};
use proptest::arbitrary::Arbitrary;
use proptest::prelude::*;
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
// compute network Policy
const CREATE_NETWORK_POLICY_CHAR: char = 'P';

const INSERT_STR: &str = "INSERT";
const SELECT_STR: &str = "SELECT";
const UPDATE_STR: &str = "UPDATE";
const DELETE_STR: &str = "DELETE";
const USAGE_STR: &str = "USAGE";
const CREATE_STR: &str = "CREATE";
const CREATE_ROLE_STR: &str = "CREATEROLE";
const CREATE_DB_STR: &str = "CREATEDB";
const CREATE_CLUSTER_STR: &str = "CREATECLUSTER";
const CREATE_NETWORK_POLICY_STR: &str = "CREATENETWORKPOLICY";

/// The OID used to represent the PUBLIC role. See:
/// <https://github.com/postgres/postgres/blob/29a0ccbce97978e5d65b8f96c85a00611bb403c4/src/include/utils/acl.h#L46>
pub const PUBLIC_ROLE_OID: Oid = Oid(0);

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
        const CREATE_NETWORK_POLICY = 1 << 32;

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
            CREATE_NETWORK_POLICY_STR => Ok(AclMode::CREATE_NETWORK_POLICY),
            _ => Err(anyhow!("{}", s.quoted())),
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
        self.explode().join(", ")
    }

    pub fn explode(&self) -> Vec<&'static str> {
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
        if self.contains(AclMode::CREATE_NETWORK_POLICY) {
            privileges.push(CREATE_NETWORK_POLICY_STR);
        }
        privileges
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
                CREATE_NETWORK_POLICY_CHAR => acl_mode.bitor_assign(AclMode::CREATE_NETWORK_POLICY),
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
        if self.contains(AclMode::CREATE_NETWORK_POLICY) {
            write!(f, "{CREATE_NETWORK_POLICY_CHAR}")?;
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
    type InnerRegion = CopyRegion<AclMode>;
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
}

impl FromStr for MzAclItem {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<_> = s.split('=').collect();
        let &[grantee, rest] = parts.as_slice() else {
            return Err(anyhow!("invalid mz_aclitem '{s}'"));
        };

        let parts: Vec<_> = rest.split('/').collect();
        let &[acl_mode, grantor] = parts.as_slice() else {
            return Err(anyhow!("invalid mz_aclitem '{s}'"));
        };

        let grantee: RoleId = if grantee.is_empty() {
            RoleId::Public
        } else {
            grantee.parse()?
        };
        let acl_mode: AclMode = acl_mode.parse()?;
        let grantor: RoleId = grantor.parse()?;

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
    type InnerRegion = CopyRegion<MzAclItem>;
}

/// An encoded packed variant of [`MzAclItem`].
///
/// We uphold the variant that [`PackedMzAclItem`] sorts the same as [`MzAclItem`].
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PackedMzAclItem([u8; Self::SIZE]);

impl PackedMzAclItem {
    // Note: It's critical to the sort order guarantee that these tags sort the same as the
    // variants of RoleId. We leave space between each tag for future variants.
    //
    // Note: An alternative would be to use a `u8` for the tag, while this is more memory
    // efficient micro benchmarks show throughput increases by ~40% for the encode path and ~130%
    // for the decode path when using a `u32`. Looking at the generated assembly there are more
    // load and store instructions when using a `u8` and unaligned reads.

    pub const SYSTEM_TAG: u32 = 100;
    pub const PREDEFINED_TAG: u32 = 200;
    pub const USER_TAG: u32 = 300;
    pub const PUBLIC_TAG: u32 = 400;

    #[inline]
    fn encode_role(buf: &mut [u8], role: RoleId) {
        soft_assert_no_log!(buf.len() == 12);

        match role {
            RoleId::System(val) => {
                buf[..4].copy_from_slice(&Self::SYSTEM_TAG.to_be_bytes());
                buf[4..].copy_from_slice(&val.to_be_bytes());
            }
            RoleId::Predefined(val) => {
                buf[..4].copy_from_slice(&Self::PREDEFINED_TAG.to_be_bytes());
                buf[4..].copy_from_slice(&val.to_be_bytes());
            }
            RoleId::User(val) => {
                buf[..4].copy_from_slice(&Self::USER_TAG.to_be_bytes());
                buf[4..].copy_from_slice(&val.to_be_bytes());
            }
            RoleId::Public => {
                buf[..4].copy_from_slice(&Self::PUBLIC_TAG.to_be_bytes());
            }
        }
    }

    #[inline]
    fn decode_role(buf: &[u8]) -> RoleId {
        soft_assert_no_log!(buf.len() == 12);

        let tag: [u8; 4] = buf[..4]
            .try_into()
            .expect("PackedMzAclItem should roundtrip");
        let tag = u32::from_be_bytes(tag);

        let val: [u8; 8] = buf[4..]
            .try_into()
            .expect("PackedMzAclItem should roundtrip");
        let val = u64::from_be_bytes(val);

        match tag {
            Self::SYSTEM_TAG => RoleId::System(val),
            Self::PREDEFINED_TAG => RoleId::Predefined(val),
            Self::USER_TAG => RoleId::User(val),
            Self::PUBLIC_TAG => RoleId::Public,
            x => panic!("unrecognized tag {x}"),
        }
    }
}

impl FixedSizeCodec<MzAclItem> for PackedMzAclItem {
    const SIZE: usize = 32;

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    fn from_bytes(val: &[u8]) -> Result<Self, String>
    where
        Self: Sized,
    {
        let buf: [u8; Self::SIZE] = val.try_into().map_err(|_| {
            format!(
                "size for PackedMzAclItem is {} bytes, got {}",
                Self::SIZE,
                val.len()
            )
        })?;

        Ok(PackedMzAclItem(buf))
    }

    #[inline]
    fn from_value(value: MzAclItem) -> Self {
        let mut buf = [0u8; 32];

        Self::encode_role(&mut buf[..12], value.grantee);
        Self::encode_role(&mut buf[12..24], value.grantor);
        buf[24..].copy_from_slice(&value.acl_mode.bits().to_be_bytes());

        PackedMzAclItem(buf)
    }

    #[inline]
    fn into_value(self) -> MzAclItem {
        let grantee = PackedMzAclItem::decode_role(&self.0[..12]);
        let grantor = PackedMzAclItem::decode_role(&self.0[12..24]);

        let acl_mode: [u8; 8] = self.0[24..]
            .try_into()
            .expect("PackedMzAclItem should roundtrip");
        let acl_mode = AclMode::from_bits(u64::from_be_bytes(acl_mode))
            .expect("PackedMzAclItem should roundtrip");

        MzAclItem {
            grantee,
            grantor,
            acl_mode,
        }
    }
}

/// A list of privileges granted to a role.
///
/// This is primarily used for compatibility in PostgreSQL.
///
/// See: <https://github.com/postgres/postgres/blob/7f5b19817eaf38e70ad1153db4e644ee9456853e/src/include/utils/acl.h#L48-L59>
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Hash, Deserialize, Arbitrary,
)]
pub struct AclItem {
    /// Role that this item grants privileges to.
    pub grantee: Oid,
    /// Grantor of privileges.
    pub grantor: Oid,
    /// Privileges bit flag.
    pub acl_mode: AclMode,
}

impl AclItem {
    pub fn empty(grantee: Oid, grantor: Oid) -> AclItem {
        AclItem {
            grantee,
            grantor,
            acl_mode: AclMode::empty(),
        }
    }
}

// PostgreSQL has no stable binary encoding for AclItems. However, we need to be able to convert
// types to and from binary, to pack a Datum in a row, so we invent our own encoding. The encoding
// matches the in memory format that PostgreSQL uses, which is (Oid, Oid, AclMode).
impl AclItem {
    pub fn encode_binary(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(Self::binary_size());
        res.extend_from_slice(&self.grantee.0.to_le_bytes());
        res.extend_from_slice(&self.grantor.0.to_le_bytes());
        res.extend_from_slice(&self.acl_mode.bits().to_le_bytes());
        res
    }

    pub fn decode_binary(raw: &[u8]) -> Result<AclItem, Error> {
        if raw.len() != AclItem::binary_size() {
            return Err(anyhow!(
                "invalid binary size, expecting {}, found {}",
                AclItem::binary_size(),
                raw.len()
            ));
        }

        let oid_size = size_of::<u32>();

        let grantee = Oid(u32::from_le_bytes(raw[0..oid_size].try_into()?));
        let raw = &raw[oid_size..];
        let grantor = Oid(u32::from_le_bytes(raw[0..oid_size].try_into()?));
        let raw = &raw[oid_size..];
        let acl_mode = u64::from_le_bytes(raw.try_into()?);

        Ok(AclItem {
            grantee,
            grantor,
            acl_mode: AclMode { bits: acl_mode },
        })
    }

    pub const fn binary_size() -> usize {
        size_of::<u32>() + size_of::<u32>() + size_of::<u64>()
    }
}

impl FromStr for AclItem {
    type Err = Error;

    /// For the PostgreSQL implementation see:
    ///   * <https://github.com/postgres/postgres/blob/9089287aa037fdecb5a52cec1926e5ae9569e9f9/src/backend/utils/adt/acl.c#L580-L609>
    ///   * <https://github.com/postgres/postgres/blob/9089287aa037fdecb5a52cec1926e5ae9569e9f9/src/backend/utils/adt/acl.c#L223-L390>
    ///   * <https://github.com/postgres/postgres/blob/9089287aa037fdecb5a52cec1926e5ae9569e9f9/src/backend/utils/adt/acl.c#L127-L187>
    ///
    /// PostgreSQL is able to look up OIDs in the catalog while parsing, so it accepts aclitems that
    /// contain role OIDs or role names. We have no way to access the catalog here, so we can only
    /// accept oids. Therefore our implementation is much simpler than PostgreSQL's.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<_> = s.split('=').collect();
        let &[grantee, rest] = parts.as_slice() else {
            return Err(anyhow!("invalid aclitem '{s}'"));
        };

        let parts: Vec<_> = rest.split('/').collect();
        let &[acl_mode, grantor] = parts.as_slice() else {
            return Err(anyhow!("invalid mz_aclitem '{s}'"));
        };

        let grantee: Oid = if grantee.is_empty() {
            PUBLIC_ROLE_OID
        } else {
            Oid(grantee.parse()?)
        };
        let acl_mode: AclMode = acl_mode.parse()?;
        let grantor: Oid = Oid(grantor.parse()?);

        Ok(AclItem {
            grantee,
            grantor,
            acl_mode,
        })
    }
}

impl fmt::Display for AclItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.grantee != PUBLIC_ROLE_OID {
            write!(f, "{}", self.grantee.0)?;
        }
        write!(f, "={}/{}", self.acl_mode, self.grantor.0)
    }
}

impl RustType<ProtoAclItem> for AclItem {
    fn into_proto(&self) -> ProtoAclItem {
        ProtoAclItem {
            grantee: self.grantee.0,
            grantor: self.grantor.0,
            acl_mode: Some(self.acl_mode.into_proto()),
        }
    }

    fn from_proto(proto: ProtoAclItem) -> Result<Self, TryFromProtoError> {
        match proto.acl_mode {
            Some(acl_mode) => Ok(AclItem {
                grantee: Oid(proto.grantee),
                grantor: Oid(proto.grantor),
                acl_mode: AclMode::from_proto(acl_mode)?,
            }),
            None => Err(TryFromProtoError::missing_field("ProtoMzAclItem::acl_mode")),
        }
    }
}

impl Columnation for AclItem {
    type InnerRegion = CopyRegion<AclItem>;
}

/// An encoded packed variant of [`AclItem`].
///
/// We uphold the variant that [`PackedAclItem`] sorts the same as [`AclItem`].
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PackedAclItem([u8; Self::SIZE]);

impl FixedSizeCodec<AclItem> for PackedAclItem {
    const SIZE: usize = 16;

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    fn from_bytes(slice: &[u8]) -> Result<Self, String> {
        let buf: [u8; Self::SIZE] = slice.try_into().map_err(|_| {
            format!(
                "size for PackedAclItem is {} bytes, got {}",
                Self::SIZE,
                slice.len()
            )
        })?;
        Ok(PackedAclItem(buf))
    }

    #[inline]
    fn from_value(value: AclItem) -> Self {
        let mut buf = [0u8; 16];

        buf[..4].copy_from_slice(&value.grantee.0.to_be_bytes());
        buf[4..8].copy_from_slice(&value.grantor.0.to_be_bytes());
        buf[8..].copy_from_slice(&value.acl_mode.bits().to_be_bytes());

        PackedAclItem(buf)
    }

    #[inline]
    fn into_value(self) -> AclItem {
        let mut grantee = [0; 4];
        grantee.copy_from_slice(&self.0[..4]);

        let mut grantor = [0; 4];
        grantor.copy_from_slice(&self.0[4..8]);

        let mut acl_mode = [0; 8];
        acl_mode.copy_from_slice(&self.0[8..]);
        let acl_mode = AclMode::from_bits(u64::from_be_bytes(acl_mode))
            .expect("PackedAclItem should roundtrip");

        AclItem {
            grantee: Oid(u32::from_be_bytes(grantee)),
            grantor: Oid(u32::from_be_bytes(grantor)),
            acl_mode,
        }
    }
}

/// A container of [`MzAclItem`]s that is optimized to look up an [`MzAclItem`] by the grantee.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct PrivilegeMap(
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")] BTreeMap<RoleId, Vec<MzAclItem>>,
);

impl PrivilegeMap {
    /// Creates a new empty `PrivilegeMap`.
    pub fn new() -> PrivilegeMap {
        PrivilegeMap(BTreeMap::new())
    }

    /// Creates a new `PrivilegeMap` from a collection of [`MzAclItem`]s.
    pub fn from_mz_acl_items(items: impl IntoIterator<Item = MzAclItem>) -> PrivilegeMap {
        let mut map = PrivilegeMap::new();
        map.grant_all(items);
        map
    }

    /// Get the acl item granted to `grantee` by `grantor`.
    pub fn get_acl_item(&self, grantee: &RoleId, grantor: &RoleId) -> Option<&MzAclItem> {
        self.0.get(grantee).and_then(|privileges| {
            privileges
                .into_iter()
                .find(|mz_acl_item| &mz_acl_item.grantor == grantor)
        })
    }

    /// Get all acl items granted to `grantee`.
    pub fn get_acl_items_for_grantee(&self, grantee: &RoleId) -> impl Iterator<Item = &MzAclItem> {
        self.0
            .get(grantee)
            .into_iter()
            .flat_map(|privileges| privileges.into_iter())
    }

    /// Returns references to all contained [`MzAclItem`].
    pub fn all_values(&self) -> impl Iterator<Item = &MzAclItem> {
        self.0
            .values()
            .flat_map(|privileges| privileges.into_iter())
    }

    /// Returns clones of all contained [`MzAclItem`].
    pub fn all_values_owned(&self) -> impl Iterator<Item = MzAclItem> + '_ {
        self.all_values().cloned()
    }

    /// Consumes self and returns all contained [`MzAclItem`].
    pub fn into_all_values(self) -> impl Iterator<Item = MzAclItem> {
        self.0
            .into_values()
            .flat_map(|privileges| privileges.into_iter())
    }

    /// Adds an [`MzAclItem`] to this map.
    pub fn grant(&mut self, privilege: MzAclItem) {
        let grantee_privileges = self.0.entry(privilege.grantee).or_default();
        if let Some(existing_privilege) = grantee_privileges
            .iter_mut()
            .find(|cur_privilege| cur_privilege.grantor == privilege.grantor)
        {
            // sanity check that the key is consistent.
            assert_eq!(
                privilege.grantee, existing_privilege.grantee,
                "PrivilegeMap out of sync"
            );
            existing_privilege.acl_mode = existing_privilege.acl_mode.union(privilege.acl_mode);
        } else {
            grantee_privileges.push(privilege);
        }
    }

    /// Grant multiple [`MzAclItem`]s to this map.
    pub fn grant_all(&mut self, mz_acl_items: impl IntoIterator<Item = MzAclItem>) {
        for mz_acl_item in mz_acl_items {
            self.grant(mz_acl_item);
        }
    }

    /// Removes an [`MzAclItem`] from this map.
    pub fn revoke(&mut self, privilege: &MzAclItem) {
        let grantee_privileges = self.0.entry(privilege.grantee).or_default();
        if let Some(existing_privilege) = grantee_privileges
            .iter_mut()
            .find(|cur_privilege| cur_privilege.grantor == privilege.grantor)
        {
            // sanity check that the key is consistent.
            assert_eq!(
                privilege.grantee, existing_privilege.grantee,
                "PrivilegeMap out of sync"
            );
            existing_privilege.acl_mode =
                existing_privilege.acl_mode.difference(privilege.acl_mode);
        }

        // Remove empty privileges.
        grantee_privileges.retain(|privilege| !privilege.acl_mode.is_empty());
        if grantee_privileges.is_empty() {
            self.0.remove(&privilege.grantee);
        }
    }

    /// Returns a `PrivilegeMap` formatted as a `serde_json::Value` that is suitable for debugging. For
    /// example `CatalogState::dump`.
    pub fn debug_json(&self) -> serde_json::Value {
        let privileges_by_str: BTreeMap<String, _> = self
            .0
            .iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect();
        serde_json::json!(privileges_by_str)
    }
}

impl Default for PrivilegeMap {
    fn default() -> PrivilegeMap {
        PrivilegeMap::new()
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
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_NETWORK_POLICY));
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
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_NETWORK_POLICY));
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
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_NETWORK_POLICY));
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
    assert!(!mz_acl.acl_mode.contains(AclMode::CREATE_NETWORK_POLICY));
    assert_eq!(s, mz_acl.to_string());

    let s = "u1=RBNP/u2";
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
    assert!(mz_acl.acl_mode.contains(AclMode::CREATE_NETWORK_POLICY));
    assert_eq!(s, mz_acl.to_string());

    mz_ore::assert_err!("u42/rw=u666".parse::<MzAclItem>());
    mz_ore::assert_err!("u32=C/".parse::<MzAclItem>());
    mz_ore::assert_err!("=/".parse::<MzAclItem>());
    mz_ore::assert_err!("f62hfiuew827fhh".parse::<MzAclItem>());
    mz_ore::assert_err!("u2=rw/s66=CU/u33".parse::<MzAclItem>());
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

    mz_ore::assert_err!(MzAclItem::decode_binary(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 0]))
}

#[mz_ore::test]
fn test_mz_acl_item_binary_size() {
    assert_eq!(26, MzAclItem::binary_size());
}

#[mz_ore::test]
fn test_acl_parsing() {
    let s = "42=rw/666";
    let acl: AclItem = s.parse().unwrap();
    assert_eq!(42, acl.grantee.0);
    assert_eq!(666, acl.grantor.0);
    assert!(!acl.acl_mode.contains(AclMode::INSERT));
    assert!(acl.acl_mode.contains(AclMode::SELECT));
    assert!(acl.acl_mode.contains(AclMode::UPDATE));
    assert!(!acl.acl_mode.contains(AclMode::DELETE));
    assert!(!acl.acl_mode.contains(AclMode::USAGE));
    assert!(!acl.acl_mode.contains(AclMode::CREATE));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_ROLE));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_DB));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_CLUSTER));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_NETWORK_POLICY));
    assert_eq!(s, acl.to_string());

    let s = "=UC/4";
    let acl: AclItem = s.parse().unwrap();
    assert_eq!(PUBLIC_ROLE_OID, acl.grantee);
    assert_eq!(4, acl.grantor.0);
    assert!(!acl.acl_mode.contains(AclMode::INSERT));
    assert!(!acl.acl_mode.contains(AclMode::SELECT));
    assert!(!acl.acl_mode.contains(AclMode::UPDATE));
    assert!(!acl.acl_mode.contains(AclMode::DELETE));
    assert!(acl.acl_mode.contains(AclMode::USAGE));
    assert!(acl.acl_mode.contains(AclMode::CREATE));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_ROLE));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_DB));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_CLUSTER));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_NETWORK_POLICY));
    assert_eq!(s, acl.to_string());

    let s = "7=/12";
    let acl: AclItem = s.parse().unwrap();
    assert_eq!(7, acl.grantee.0);
    assert_eq!(12, acl.grantor.0);
    assert!(!acl.acl_mode.contains(AclMode::INSERT));
    assert!(!acl.acl_mode.contains(AclMode::SELECT));
    assert!(!acl.acl_mode.contains(AclMode::UPDATE));
    assert!(!acl.acl_mode.contains(AclMode::DELETE));
    assert!(!acl.acl_mode.contains(AclMode::USAGE));
    assert!(!acl.acl_mode.contains(AclMode::CREATE));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_ROLE));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_DB));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_CLUSTER));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_NETWORK_POLICY));
    assert_eq!(s, acl.to_string());

    let s = "=/100";
    let acl: AclItem = s.parse().unwrap();
    assert_eq!(PUBLIC_ROLE_OID, acl.grantee);
    assert_eq!(100, acl.grantor.0);
    assert!(!acl.acl_mode.contains(AclMode::INSERT));
    assert!(!acl.acl_mode.contains(AclMode::SELECT));
    assert!(!acl.acl_mode.contains(AclMode::UPDATE));
    assert!(!acl.acl_mode.contains(AclMode::DELETE));
    assert!(!acl.acl_mode.contains(AclMode::USAGE));
    assert!(!acl.acl_mode.contains(AclMode::CREATE));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_ROLE));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_DB));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_CLUSTER));
    assert!(!acl.acl_mode.contains(AclMode::CREATE_NETWORK_POLICY));
    assert_eq!(s, acl.to_string());

    let s = "1=RBNP/2";
    let acl: AclItem = s.parse().unwrap();
    assert_eq!(1, acl.grantee.0);
    assert_eq!(2, acl.grantor.0);
    assert!(!acl.acl_mode.contains(AclMode::INSERT));
    assert!(!acl.acl_mode.contains(AclMode::SELECT));
    assert!(!acl.acl_mode.contains(AclMode::UPDATE));
    assert!(!acl.acl_mode.contains(AclMode::DELETE));
    assert!(!acl.acl_mode.contains(AclMode::USAGE));
    assert!(!acl.acl_mode.contains(AclMode::CREATE));
    assert!(acl.acl_mode.contains(AclMode::CREATE_ROLE));
    assert!(acl.acl_mode.contains(AclMode::CREATE_DB));
    assert!(acl.acl_mode.contains(AclMode::CREATE_CLUSTER));
    assert!(acl.acl_mode.contains(AclMode::CREATE_NETWORK_POLICY));
    assert_eq!(s, acl.to_string());

    mz_ore::assert_err!("42/rw=666".parse::<AclItem>());
    mz_ore::assert_err!("u42=rw/u666".parse::<AclItem>());
    mz_ore::assert_err!("s42=rw/s666".parse::<AclItem>());
    mz_ore::assert_err!("u32=C/".parse::<AclItem>());
    mz_ore::assert_err!("=/".parse::<AclItem>());
    mz_ore::assert_err!("f62hfiuew827fhh".parse::<AclItem>());
    mz_ore::assert_err!("u2=rw/s66=CU/u33".parse::<AclItem>());
}

#[mz_ore::test]
fn test_acl_item_binary() {
    use std::ops::BitAnd;

    let acl_item = AclItem {
        grantee: Oid(42),
        grantor: Oid(666),
        acl_mode: AclMode::empty()
            .bitand(AclMode::SELECT)
            .bitand(AclMode::UPDATE),
    };
    assert_eq!(
        acl_item,
        AclItem::decode_binary(&acl_item.encode_binary()).unwrap()
    );

    let acl_item = AclItem {
        grantee: PUBLIC_ROLE_OID,
        grantor: Oid(4),
        acl_mode: AclMode::empty()
            .bitand(AclMode::USAGE)
            .bitand(AclMode::CREATE),
    };
    assert_eq!(
        acl_item,
        AclItem::decode_binary(&acl_item.encode_binary()).unwrap()
    );

    let acl_item = AclItem {
        grantee: Oid(7),
        grantor: Oid(12),
        acl_mode: AclMode::empty(),
    };
    assert_eq!(
        acl_item,
        AclItem::decode_binary(&acl_item.encode_binary()).unwrap()
    );

    let acl_item = AclItem {
        grantee: PUBLIC_ROLE_OID,
        grantor: Oid(100),
        acl_mode: AclMode::empty(),
    };
    assert_eq!(
        acl_item,
        AclItem::decode_binary(&acl_item.encode_binary()).unwrap()
    );

    mz_ore::assert_err!(AclItem::decode_binary(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 0]))
}

#[mz_ore::test]
fn test_acl_item_binary_size() {
    assert_eq!(16, AclItem::binary_size());
}

proptest! {
  #[mz_ore::test]
  #[cfg_attr(miri, ignore)] // slow
  fn proptest_acl_item_binary_encoding_roundtrip(acl_item: AclItem) {
      let encoded = acl_item.encode_binary();
      let decoded = AclItem::decode_binary(&encoded).unwrap();
      assert_eq!(acl_item, decoded);
  }

  #[mz_ore::test]
  #[cfg_attr(miri, ignore)] // slow
  fn proptest_valid_acl_item_str(acl_item: AclItem) {
      let encoded = acl_item.to_string();
      let decoded = AclItem::from_str(&encoded).unwrap();
      assert_eq!(acl_item, decoded);
  }
}

#[mz_ore::test]
fn proptest_packed_acl_item_roundtrips() {
    fn roundtrip_acl_item(og: AclItem) {
        let packed = PackedAclItem::from_value(og);
        let rnd = packed.into_value();
        assert_eq!(og, rnd);
    }

    proptest!(|(acl_item in proptest::arbitrary::any::<AclItem>())| {
        roundtrip_acl_item(acl_item);
    })
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // slow
fn proptest_packed_acl_item_sorts() {
    fn sort_acl_items(mut og: Vec<AclItem>) {
        let mut packed: Vec<_> = og.iter().copied().map(PackedAclItem::from_value).collect();

        og.sort();
        packed.sort();

        let rnd: Vec<_> = packed.into_iter().map(PackedAclItem::into_value).collect();
        assert_eq!(og, rnd);
    }

    proptest!(|(acl_items in proptest::collection::vec(any::<AclItem>(), 0..64))| {
        sort_acl_items(acl_items);
    });
}

#[mz_ore::test]
fn proptest_packed_mz_acl_item_roundtrips() {
    fn roundtrip_mz_acl_item(og: MzAclItem) {
        let packed = PackedMzAclItem::from_value(og);
        let rnd = packed.into_value();
        assert_eq!(og, rnd);
    }

    proptest!(|(acl_item in proptest::arbitrary::any::<MzAclItem>())| {
        roundtrip_mz_acl_item(acl_item);
    })
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // slow
fn proptest_packed_mz_acl_item_sorts() {
    fn sort_mz_acl_items(mut og: Vec<MzAclItem>) {
        let mut packed: Vec<_> = og
            .iter()
            .copied()
            .map(PackedMzAclItem::from_value)
            .collect();

        og.sort();
        packed.sort();

        let rnd: Vec<_> = packed
            .into_iter()
            .map(PackedMzAclItem::into_value)
            .collect();
        assert_eq!(og, rnd);
    }

    proptest!(|(acl_items in proptest::collection::vec(any::<MzAclItem>(), 0..64))| {
        sort_mz_acl_items(acl_items);
    });
}

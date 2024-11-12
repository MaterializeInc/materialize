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
use mz_lowertest::MzReflect;
use mz_proto::{RustType, TryFromProtoError};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

include!(concat!(env!("OUT_DIR"), "/mz_repr.catalog_item_id.rs"));

/// The identifier for an item within the Catalog.
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
pub enum CatalogItemId {
    /// System namespace.
    System(u64),
    /// User namespace.
    User(u64),
    /// Transient item.
    Transient(u64),
}

impl CatalogItemId {
    /// Reports whether this ID is in the system namespace.
    pub fn is_system(&self) -> bool {
        matches!(self, CatalogItemId::System(_))
    }

    /// Reports whether this ID is in the user namespace.
    pub fn is_user(&self) -> bool {
        matches!(self, CatalogItemId::User(_))
    }

    /// Reports whether this ID is for a transient item.
    pub fn is_transient(&self) -> bool {
        matches!(self, CatalogItemId::Transient(_))
    }
}

impl FromStr for CatalogItemId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            return Err(anyhow!("couldn't parse id {}", s));
        }
        let val: u64 = s[1..].parse()?;
        match s.chars().next().unwrap() {
            's' => Ok(CatalogItemId::System(val)),
            'u' => Ok(CatalogItemId::User(val)),
            't' => Ok(CatalogItemId::Transient(val)),
            _ => Err(anyhow!("couldn't parse id {}", s)),
        }
    }
}

impl fmt::Display for CatalogItemId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CatalogItemId::System(id) => write!(f, "s{}", id),
            CatalogItemId::User(id) => write!(f, "u{}", id),
            CatalogItemId::Transient(id) => write!(f, "t{}", id),
        }
    }
}

impl RustType<ProtoCatalogItemId> for CatalogItemId {
    fn into_proto(&self) -> ProtoCatalogItemId {
        use proto_catalog_item_id::Kind::*;
        ProtoCatalogItemId {
            kind: Some(match self {
                CatalogItemId::System(x) => System(*x),
                CatalogItemId::User(x) => User(*x),
                CatalogItemId::Transient(x) => Transient(*x),
            }),
        }
    }

    fn from_proto(proto: ProtoCatalogItemId) -> Result<Self, TryFromProtoError> {
        use proto_catalog_item_id::Kind::*;
        match proto.kind {
            Some(System(x)) => Ok(CatalogItemId::System(x)),
            Some(User(x)) => Ok(CatalogItemId::User(x)),
            Some(Transient(x)) => Ok(CatalogItemId::Transient(x)),
            None => Err(TryFromProtoError::missing_field("ProtoCatalogItemId::kind")),
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_proto::ProtoType;
    use proptest::prelude::*;
    use prost::Message;

    use super::*;
    use crate::GlobalId;

    #[mz_ore::test]
    fn proptest_catalog_item_id_roundtrips() {
        fn testcase(og: CatalogItemId) {
            let s = og.to_string();
            let rnd: CatalogItemId = s.parse().unwrap();
            assert_eq!(og, rnd);
        }

        proptest!(|(id in any::<CatalogItemId>())| {
            testcase(id);
        })
    }

    #[mz_ore::test]
    fn proptest_catalog_item_id_global_id_wire_compat() {
        fn testcase(og: GlobalId) {
            let bytes = og.into_proto().encode_to_vec();
            let proto = ProtoCatalogItemId::decode(&bytes[..]).expect("valid");
            let rnd = proto.into_rust().expect("valid proto");

            match (og, rnd) {
                (GlobalId::User(x), CatalogItemId::User(y)) => assert_eq!(x, y),
                (GlobalId::System(x), CatalogItemId::System(y)) => assert_eq!(x, y),
                (GlobalId::Transient(x), CatalogItemId::Transient(y)) => assert_eq!(x, y),
                (gid, item) => panic!("{gid:?} turned into {item:?}"),
            }
        }

        let strat = proptest::arbitrary::any::<u64>().prop_flat_map(|inner| {
            proptest::strategy::Union::new(vec![
                Just(GlobalId::User(inner)),
                Just(GlobalId::System(inner)),
                Just(GlobalId::Transient(inner)),
            ])
        });
        proptest!(|(id in strat)| {
            testcase(id)
        });
    }
}

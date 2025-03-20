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
    /// Introspection Source Index namespace.
    IntrospectionSourceIndex(u64),
    /// User namespace.
    User(u64),
    /// Transient item.
    Transient(u64),
}

impl CatalogItemId {
    /// Reports whether this ID is in the system namespace.
    pub fn is_system(&self) -> bool {
        matches!(
            self,
            CatalogItemId::System(_) | CatalogItemId::IntrospectionSourceIndex(_)
        )
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

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            return Err(anyhow!("couldn't parse id {}", s));
        }
        let tag = s.chars().next().unwrap();
        s = &s[1..];
        let variant = match tag {
            's' => {
                if Some('i') == s.chars().next() {
                    s = &s[1..];
                    CatalogItemId::IntrospectionSourceIndex
                } else {
                    CatalogItemId::System
                }
            }
            'u' => CatalogItemId::User,
            't' => CatalogItemId::Transient,
            _ => return Err(anyhow!("couldn't parse id {}", s)),
        };
        let val: u64 = s.parse()?;
        Ok(variant(val))
    }
}

impl fmt::Display for CatalogItemId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CatalogItemId::System(id) => write!(f, "s{}", id),
            CatalogItemId::IntrospectionSourceIndex(id) => write!(f, "si{}", id),
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
                CatalogItemId::IntrospectionSourceIndex(x) => IntrospectionSourceIndex(*x),
                CatalogItemId::User(x) => User(*x),
                CatalogItemId::Transient(x) => Transient(*x),
            }),
        }
    }

    fn from_proto(proto: ProtoCatalogItemId) -> Result<Self, TryFromProtoError> {
        use proto_catalog_item_id::Kind::*;
        match proto.kind {
            Some(System(x)) => Ok(CatalogItemId::System(x)),
            Some(IntrospectionSourceIndex(x)) => Ok(CatalogItemId::IntrospectionSourceIndex(x)),
            Some(User(x)) => Ok(CatalogItemId::User(x)),
            Some(Transient(x)) => Ok(CatalogItemId::Transient(x)),
            None => Err(TryFromProtoError::missing_field("ProtoCatalogItemId::kind")),
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

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
}

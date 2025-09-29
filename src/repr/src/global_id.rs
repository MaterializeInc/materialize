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
use columnar::Columnar;
use columnation::{Columnation, CopyRegion};
use mz_lowertest::MzReflect;
use mz_ore::id_gen::AtomicIdGen;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::CatalogItemId;

include!(concat!(env!("OUT_DIR"), "/mz_repr.global_id.rs"));

/// The identifier for an item/object.
///
/// WARNING: `GlobalId`'s `Ord` implementation does not express a dependency order.
/// One should explicitly topologically sort objects by their dependencies, rather
/// than rely on the order of identifiers.
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
    Columnar,
)]
pub enum GlobalId {
    /// System namespace.
    System(u64),
    /// Introspection Source Index namespace.
    IntrospectionSourceIndex(u64),
    /// User namespace.
    User(u64),
    /// Transient namespace.
    Transient(u64),
    /// Dummy id for query being explained
    Explain,
}

// `GlobalId`s are serialized often, so it would be nice to try and keep them small. If this assert
// fails, then there isn't any correctness issues just potential performance issues.
static_assertions::assert_eq_size!(GlobalId, [u8; 16]);

impl GlobalId {
    /// Reports whether this ID is in the system namespace.
    pub fn is_system(&self) -> bool {
        matches!(
            self,
            GlobalId::System(_) | GlobalId::IntrospectionSourceIndex(_)
        )
    }

    /// Reports whether this ID is in the user namespace.
    pub fn is_user(&self) -> bool {
        matches!(self, GlobalId::User(_))
    }

    /// Reports whether this ID is in the transient namespace.
    pub fn is_transient(&self) -> bool {
        matches!(self, GlobalId::Transient(_))
    }
}

impl FromStr for GlobalId {
    type Err = Error;

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            return Err(anyhow!("couldn't parse id {}", s));
        }
        if s == "Explained Query" {
            return Ok(GlobalId::Explain);
        }
        let tag = s.chars().next().unwrap();
        s = &s[1..];
        let variant = match tag {
            's' => {
                if Some('i') == s.chars().next() {
                    s = &s[1..];
                    GlobalId::IntrospectionSourceIndex
                } else {
                    GlobalId::System
                }
            }
            'u' => GlobalId::User,
            't' => GlobalId::Transient,
            _ => return Err(anyhow!("couldn't parse id {}", s)),
        };
        let val: u64 = s.parse()?;
        Ok(variant(val))
    }
}

impl fmt::Display for GlobalId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GlobalId::System(id) => write!(f, "s{}", id),
            GlobalId::IntrospectionSourceIndex(id) => write!(f, "si{}", id),
            GlobalId::User(id) => write!(f, "u{}", id),
            GlobalId::Transient(id) => write!(f, "t{}", id),
            GlobalId::Explain => write!(f, "Explained Query"),
        }
    }
}

impl Columnation for GlobalId {
    type InnerRegion = CopyRegion<GlobalId>;
}

#[derive(Debug)]
pub struct TransientIdGen(AtomicIdGen);

impl TransientIdGen {
    pub fn new() -> Self {
        let inner = AtomicIdGen::default();
        // Transient IDs start at 1, so throw away the 0 value.
        let _ = inner.allocate_id();
        Self(inner)
    }

    pub fn allocate_id(&self) -> (CatalogItemId, GlobalId) {
        let inner = self.0.allocate_id();
        (CatalogItemId::Transient(inner), GlobalId::Transient(inner))
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::ops::Range;
use std::str::FromStr;

use anyhow::{anyhow, Error};
use columnation::{Columnation, CopyRegion};
use mz_lowertest::MzReflect;
use mz_ore::id_gen::AtomicIdGen;
use mz_proto::{RustType, TryFromProtoError};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::CatalogItemId;

include!(concat!(env!("OUT_DIR"), "/mz_repr.global_id.rs"));

/// The identifier for an item/object.
///
/// WARNING: `GlobalId`'s `Ord` implementation has at various times expressed:
/// - Dependency ordering (items with greater `GlobalId`s can depend on those
/// lesser but never the other way around)
/// - Nothing at all regarding dependencies
///
/// Currently, `GlobalId`s express a dependency ordering. We hope to keep it
/// that way.
///
/// Before breaking this invariant, you should strongly consider alternative
/// designs, i.e. it is an intense "smell" if you need to allow inverted
/// `GlobalId` dependencies. Most likely, at some point in the future, you will
/// need to invert the dependency structure again and will regret having broken
/// it in the first place.
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
pub enum GlobalId {
    /// System namespace.
    System(SystemGlobalId),
    /// User namespace.
    User(u64),
    /// Transient namespace.
    Transient(u64),
    /// Dummy id for query being explained
    Explain,
}

impl GlobalId {
    /// Reports whether this ID is in the system namespace.
    pub fn is_system(&self) -> bool {
        matches!(self, GlobalId::System(_))
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

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            return Err(anyhow!("couldn't parse id {}", s));
        }
        if s == "Explained Query" {
            return Ok(GlobalId::Explain);
        }
        match s.chars().next().unwrap() {
            's' => Ok(GlobalId::System(s[1..].parse()?)),
            'u' => Ok(GlobalId::User(s[1..].parse()?)),
            't' => Ok(GlobalId::Transient(s[1..].parse()?)),
            _ => Err(anyhow!("couldn't parse id {}", s)),
        }
    }
}

impl fmt::Display for GlobalId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GlobalId::System(id) => write!(f, "s{}", id),
            GlobalId::User(id) => write!(f, "u{}", id),
            GlobalId::Transient(id) => write!(f, "t{}", id),
            GlobalId::Explain => write!(f, "Explained Query"),
        }
    }
}

impl RustType<ProtoGlobalId> for GlobalId {
    fn into_proto(&self) -> ProtoGlobalId {
        use proto_global_id::Kind::*;
        ProtoGlobalId {
            kind: Some(match self {
                GlobalId::System(x) => System(x.0),
                GlobalId::User(x) => User(*x),
                GlobalId::Transient(x) => Transient(*x),
                GlobalId::Explain => Explain(()),
            }),
        }
    }

    fn from_proto(proto: ProtoGlobalId) -> Result<Self, TryFromProtoError> {
        use proto_global_id::Kind::*;
        match proto.kind {
            Some(System(x)) => Ok(GlobalId::System(SystemGlobalId(x))),
            Some(User(x)) => Ok(GlobalId::User(x)),
            Some(Transient(x)) => Ok(GlobalId::Transient(x)),
            Some(Explain(_)) => Ok(GlobalId::Explain),
            None => Err(TryFromProtoError::missing_field("ProtoGlobalId::kind")),
        }
    }
}

impl Columnation for GlobalId {
    type InnerRegion = CopyRegion<GlobalId>;
}

/// A new type for the system variant of [`GlobalId`]s which upholds the invariant that a system
/// [`GlobalId`] allocated by deploy generation dg1 will be less than a system [`GlobalId`] allocated
/// by deploy generation dg2 if dg1 is less than dg2. i.e.
///
///     sgid1 < sgid2 if dg1 < dg2
///
/// The most significant 32 bits of [`SystemGlobalId`] is the deploy generation that allocated the
/// ID. The least significant 32 bits is the ID within that deploy generation.
#[derive(
    Arbitrary,
    Default,
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
pub struct SystemGlobalId(u64);

impl SystemGlobalId {
    pub fn new(deploy_generation: u32) -> Self {
        SystemGlobalId(u64::from(deploy_generation) << 32)
    }

    pub fn deploy_generation(&self) -> u32 {
        (self.0 >> 32).try_into().expect("will fit")
    }

    pub fn id(&self) -> u32 {
        (self.0 & 0xffff_ffff).try_into().expect("will fit")
    }

    pub fn increment_by(&self, amount: u64) -> Self {
        let global_id = SystemGlobalId(self.0 + amount);
        if global_id.deploy_generation() != self.deploy_generation() {
            panic!(
                "ran out of system global IDs for deploy generation {}",
                self.deploy_generation()
            );
        }
        global_id
    }

    pub fn range(range: Range<Self>) -> impl Iterator<Item = Self> {
        assert_eq!(
            range.start.deploy_generation(),
            range.end.deploy_generation()
        );
        (range.start.0..range.end.0).map(|inner| SystemGlobalId(inner))
    }

    pub fn from_raw(inner: u64) -> Self {
        SystemGlobalId(inner)
    }

    pub fn into_raw(self) -> u64 {
        self.0
    }
}

impl FromStr for SystemGlobalId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl fmt::Display for SystemGlobalId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<GlobalId> for SystemGlobalId {
    type Error = &'static str;

    fn try_from(val: GlobalId) -> Result<Self, Self::Error> {
        match val {
            GlobalId::System(x) => Ok(x),
            GlobalId::User(_) => Err("user"),
            GlobalId::Transient(_) => Err("transient"),
            GlobalId::Explain => Err("explain"),
        }
    }
}

impl From<SystemGlobalId> for GlobalId {
    fn from(val: SystemGlobalId) -> Self {
        GlobalId::System(val)
    }
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

#[mz_ore::test]
fn test_system_global_ids() {
    let sgid = SystemGlobalId::new(42);
    assert_eq!(sgid.deploy_generation(), 42);
    assert_eq!(sgid.id(), 0);
    let sgid = sgid.increment();
    assert_eq!(sgid.deploy_generation(), 42);
    assert_eq!(sgid.id(), 1);

    let sgid = SystemGlobalId::new(u32::MAX);
    assert_eq!(sgid.deploy_generation(), u32::MAX);
    assert_eq!(sgid.id(), 0);

    let sgid = SystemGlobalId::new(0);
    assert_eq!(sgid.deploy_generation(), 0);
    assert_eq!(sgid.id(), 0);
    let sgid = sgid.increment();
    assert_eq!(sgid.deploy_generation(), 0);
    assert_eq!(sgid.id(), 1);
}

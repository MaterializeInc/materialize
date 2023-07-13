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
use columnation::{CloneRegion, Columnation};
use mz_lowertest::MzReflect;
use mz_proto::{RustType, TryFromProtoError};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

include!(concat!(env!("OUT_DIR"), "/mz_repr.global_id.rs"));

/// The identifier for a global dataflow.
///
/// WARNING: Despite the fact that `GlobalId` implements `Ord`, the ordering of
/// IDs does not express any relationship between dependencies. We retain the
/// `Ord` implementation exclusively to facilitate placing `GlobalId`s in
/// maps/sets.
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
    System(u64),
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
        let val: u64 = s[1..].parse()?;
        match s.chars().next().unwrap() {
            's' => Ok(GlobalId::System(val)),
            'u' => Ok(GlobalId::User(val)),
            't' => Ok(GlobalId::Transient(val)),
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
                GlobalId::System(x) => System(*x),
                GlobalId::User(x) => User(*x),
                GlobalId::Transient(x) => Transient(*x),
                GlobalId::Explain => Explain(()),
            }),
        }
    }

    fn from_proto(proto: ProtoGlobalId) -> Result<Self, TryFromProtoError> {
        use proto_global_id::Kind::*;
        match proto.kind {
            Some(System(x)) => Ok(GlobalId::System(x)),
            Some(User(x)) => Ok(GlobalId::User(x)),
            Some(Transient(x)) => Ok(GlobalId::Transient(x)),
            Some(Explain(_)) => Ok(GlobalId::Explain),
            None => Err(TryFromProtoError::missing_field("ProtoGlobalId::kind")),
        }
    }
}

impl Columnation for GlobalId {
    type InnerRegion = CloneRegion<GlobalId>;
}

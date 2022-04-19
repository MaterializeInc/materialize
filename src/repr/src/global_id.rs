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
use bytes::BufMut;
use mz_persist_types::Codec;
use proptest_derive::Arbitrary;
use prost::Message;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;

use crate::proto::TryFromProtoError;

include!(concat!(env!("OUT_DIR"), "/mz_repr.global_id.rs"));

/// The identifier for a global dataflow.
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

impl From<&GlobalId> for ProtoGlobalId {
    fn from(x: &GlobalId) -> Self {
        ProtoGlobalId {
            kind: Some(match x {
                GlobalId::System(x) => proto_global_id::Kind::System(*x),
                GlobalId::User(x) => proto_global_id::Kind::User(*x),
                GlobalId::Transient(x) => proto_global_id::Kind::Transient(*x),
                GlobalId::Explain => proto_global_id::Kind::Explain(()),
            }),
        }
    }
}

impl TryFrom<ProtoGlobalId> for GlobalId {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoGlobalId) -> Result<Self, Self::Error> {
        match x.kind {
            Some(proto_global_id::Kind::System(x)) => Ok(GlobalId::System(x)),
            Some(proto_global_id::Kind::User(x)) => Ok(GlobalId::User(x)),
            Some(proto_global_id::Kind::Transient(x)) => Ok(GlobalId::Transient(x)),
            Some(proto_global_id::Kind::Explain(_)) => Ok(GlobalId::Explain),
            None => Err(TryFromProtoError::missing_field("ProtoGlobalId::kind")),
        }
    }
}

impl Codec for GlobalId {
    fn codec_name() -> String {
        "GlobalId".into()
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        let proto: ProtoGlobalId = self.into();
        Message::encode(&proto, buf).expect("provided buffer had sufficient capacity")
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        let proto: ProtoGlobalId = Message::decode(buf).map_err(|err| err.to_string())?;
        GlobalId::try_from(proto).map_err(|err| err.to_string())
    }
}

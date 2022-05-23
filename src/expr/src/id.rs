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

use anyhow::Error;
use bytes::BufMut;
use mz_repr::proto::newapi::{ProtoType, RustType};
use proptest_derive::Arbitrary;
use prost::Message;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_repr::proto::TryFromProtoError;
use mz_repr::GlobalId;

include!(concat!(env!("OUT_DIR"), "/mz_expr.id.rs"));

/// An opaque identifier for a dataflow component. In other words, identifies
/// the target of a [`MirRelationExpr::Get`](crate::MirRelationExpr::Get).
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
pub enum Id {
    /// An identifier that refers to a local component of a dataflow.
    Local(LocalId),
    /// An identifier that refers to a global dataflow.
    #[proptest(value = "Id::Global(GlobalId::System(2))")]
    Global(GlobalId),
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Id::Local(id) => id.fmt(f),
            Id::Global(id) => id.fmt(f),
        }
    }
}

impl From<&Id> for ProtoId {
    fn from(x: &Id) -> Self {
        ProtoId {
            kind: Some(match x {
                Id::Global(g) => proto_id::Kind::Global(g.into_proto()),
                Id::Local(l) => proto_id::Kind::Local(l.into()),
            }),
        }
    }
}

impl TryFrom<ProtoId> for Id {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoId) -> Result<Self, Self::Error> {
        match x.kind {
            Some(proto_id::Kind::Global(x)) => Ok(Id::Global(x.into_rust()?)),
            Some(proto_id::Kind::Local(x)) => Ok(Id::Local(x.try_into()?)),
            None => Err(TryFromProtoError::missing_field("ProtoId::kind")),
        }
    }
}

/// The identifier for a local component of a dataflow.
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
pub struct LocalId(pub(crate) u64);

impl LocalId {
    /// Constructs a new local identifier. It is the caller's responsibility
    /// to provide a unique `v`.
    pub fn new(v: u64) -> LocalId {
        LocalId(v)
    }
}

impl fmt::Display for LocalId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "l{}", self.0)
    }
}

impl From<&LocalId> for ProtoLocalId {
    fn from(x: &LocalId) -> Self {
        ProtoLocalId { value: x.0 }
    }
}

impl TryFrom<ProtoLocalId> for LocalId {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoLocalId) -> Result<Self, Self::Error> {
        Ok(LocalId::new(x.value))
    }
}

/// Unique identifier for an instantiation of a source.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SourceInstanceId {
    /// The ID of the source, shared across all instances.
    pub source_id: GlobalId,
    /// The ID of the timely dataflow containing this instantiation of this
    /// source.
    pub dataflow_id: usize,
}

impl fmt::Display for SourceInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.source_id, self.dataflow_id)
    }
}

/// Unique identifier for each part of a whole source.
///     Kafka -> partition
///     None -> sources that have no notion of partitioning (e.g file sources)
#[derive(Arbitrary, Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum PartitionId {
    Kafka(i32),
    None,
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PartitionId::Kafka(id) => write!(f, "{}", id),
            PartitionId::None => write!(f, "none"),
        }
    }
}

impl From<&PartitionId> for Option<String> {
    fn from(pid: &PartitionId) -> Option<String> {
        match pid {
            PartitionId::None => None,
            _ => Some(pid.to_string()),
        }
    }
}

impl FromStr for PartitionId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(PartitionId::None),
            s => {
                let val: i32 = s.parse()?;
                Ok(PartitionId::Kafka(val))
            }
        }
    }
}

impl From<&PartitionId> for ProtoPartitionId {
    fn from(x: &PartitionId) -> Self {
        ProtoPartitionId {
            kind: Some(match x {
                PartitionId::Kafka(x) => proto_partition_id::Kind::Kafka(*x),
                PartitionId::None => proto_partition_id::Kind::None(()),
            }),
        }
    }
}

impl TryFrom<ProtoPartitionId> for PartitionId {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoPartitionId) -> Result<Self, Self::Error> {
        match x.kind {
            Some(proto_partition_id::Kind::Kafka(x)) => Ok(PartitionId::Kafka(x)),
            Some(proto_partition_id::Kind::None(_)) => Ok(PartitionId::None),
            None => Err(TryFromProtoError::missing_field("ProtoPartitionId::kind")),
        }
    }
}

impl mz_persist_types::Codec for PartitionId {
    fn codec_name() -> String {
        "protobuf[PartitionId]".into()
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        ProtoPartitionId::from(self)
            .encode(buf)
            .expect("provided buffer had sufficient capacity")
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        ProtoPartitionId::decode(buf)
            .map_err(|err| err.to_string())?
            .try_into()
            .map_err(|err: TryFromProtoError| err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::proto::protobuf_roundtrip;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn id_protobuf_roundtrip(expect in any::<Id>()) {
            let actual = protobuf_roundtrip::<_, ProtoId>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}

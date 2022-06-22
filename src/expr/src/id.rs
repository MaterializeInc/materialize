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
use proptest_derive::Arbitrary;
use prost::Message;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_repr::proto::{ProtoType, RustType, TryFromProtoError};
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

impl RustType<ProtoId> for Id {
    fn into_proto(&self) -> ProtoId {
        ProtoId {
            kind: Some(match self {
                Id::Global(g) => proto_id::Kind::Global(g.into_proto()),
                Id::Local(l) => proto_id::Kind::Local(l.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoId) -> Result<Self, TryFromProtoError> {
        match proto.kind {
            Some(proto_id::Kind::Global(x)) => Ok(Id::Global(x.into_rust()?)),
            Some(proto_id::Kind::Local(x)) => Ok(Id::Local(x.into_rust()?)),
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

impl RustType<ProtoLocalId> for LocalId {
    fn into_proto(&self) -> ProtoLocalId {
        ProtoLocalId { value: self.0 }
    }

    fn from_proto(proto: ProtoLocalId) -> Result<Self, TryFromProtoError> {
        Ok(LocalId::new(proto.value))
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

impl RustType<ProtoPartitionId> for PartitionId {
    fn into_proto(&self) -> ProtoPartitionId {
        use proto_partition_id::Kind::*;
        ProtoPartitionId {
            kind: Some(match self {
                PartitionId::Kafka(x) => Kafka(*x),
                PartitionId::None => None(()),
            }),
        }
    }

    fn from_proto(proto: ProtoPartitionId) -> Result<Self, TryFromProtoError> {
        use proto_partition_id::Kind::*;
        match proto.kind {
            Option::Some(Kafka(x)) => Ok(PartitionId::Kafka(x)),
            Option::Some(None(_)) => Ok(PartitionId::None),
            Option::None => Err(TryFromProtoError::missing_field("ProtoPartitionId::kind")),
        }
    }
}

impl mz_persist_types::Codec for PartitionId {
    fn codec_name() -> String {
        "protobuf[PartitionId]".into()
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        self.into_proto()
            .encode(buf)
            .expect("provided buffer had sufficient capacity")
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        ProtoPartitionId::decode(buf)
            .map_err(|err| err.to_string())?
            .into_rust()
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

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use mz_lowertest::MzReflect;
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use mz_repr::GlobalId;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

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

impl From<&LocalId> for u64 {
    fn from(id: &LocalId) -> Self {
        id.0
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

#[cfg(test)]
mod tests {
    use mz_ore::assert_ok;
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[mz_ore::test]
        fn id_protobuf_roundtrip(expect in any::<Id>()) {
            let actual = protobuf_roundtrip::<_, ProtoId>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }
}

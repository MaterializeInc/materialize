// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::BufMut;
use prost::Message;

use crate::id::PartitionId;
use crate::{GlobalId, Id, LocalId};
use mz_repr::proto::TryFromProtoError;

include!(concat!(env!("OUT_DIR"), "/id.rs"));

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

impl From<&Id> for ProtoId {
    fn from(x: &Id) -> Self {
        ProtoId {
            kind: Some(match x {
                Id::Global(g) => proto_id::Kind::Global(g.into()),
                Id::Local(l) => proto_id::Kind::Local(l.into()),
            }),
        }
    }
}

impl TryFrom<ProtoId> for Id {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoId) -> Result<Self, Self::Error> {
        match x.kind {
            Some(proto_id::Kind::Global(x)) => Ok(Id::Global(x.try_into()?)),
            Some(proto_id::Kind::Local(x)) => Ok(Id::Local(x.try_into()?)),
            None => Err(TryFromProtoError::missing_field("ProtoId::kind")),
        }
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
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn id_serialization_roundtrip(original in any::<Id>()) {
            let proto = ProtoId::from(&original);
            let serialized = proto.encode_to_vec();
            let proto = ProtoId::decode(&*serialized).unwrap();
            let id = Id::try_from(proto).unwrap();
            assert_eq!(id, original);
        }
    }
}

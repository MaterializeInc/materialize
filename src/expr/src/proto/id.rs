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

include!(concat!(env!("OUT_DIR"), "/id.rs"));

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
    type Error = String;

    fn try_from(x: ProtoPartitionId) -> Result<Self, Self::Error> {
        match x.kind {
            Some(proto_partition_id::Kind::Kafka(x)) => Ok(PartitionId::Kafka(x)),
            Some(proto_partition_id::Kind::None(_)) => Ok(PartitionId::None),
            None => return Err("unknown partition_id".into()),
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
    }
}

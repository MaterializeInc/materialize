// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types stored in the storage stash collections.

include!(concat!(env!("OUT_DIR"), "/mz_storage_types.collections.rs"));

use mz_proto::{ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId as RustGlobalId, Timestamp as RustTimestamp};
use timely::progress::Antichain;

use crate::controller::DurableCollectionMetadata as RustDurableCollectionMetadata;

impl RustType<GlobalId> for RustGlobalId {
    fn into_proto(&self) -> GlobalId {
        GlobalId {
            value: Some(match self {
                RustGlobalId::System(x) => global_id::Value::System(*x),
                RustGlobalId::IntrospectionSourceIndex(x) => {
                    global_id::Value::IntrospectionSourceIndex(*x)
                }
                RustGlobalId::User(x) => global_id::Value::User(*x),
                RustGlobalId::Transient(x) => global_id::Value::Transient(*x),
                RustGlobalId::Explain => global_id::Value::Explain(Default::default()),
            }),
        }
    }

    fn from_proto(proto: GlobalId) -> Result<Self, TryFromProtoError> {
        match proto.value {
            Some(global_id::Value::System(x)) => Ok(RustGlobalId::System(x)),
            Some(global_id::Value::IntrospectionSourceIndex(x)) => {
                Ok(RustGlobalId::IntrospectionSourceIndex(x))
            }
            Some(global_id::Value::User(x)) => Ok(RustGlobalId::User(x)),
            Some(global_id::Value::Transient(x)) => Ok(RustGlobalId::Transient(x)),
            Some(global_id::Value::Explain(_)) => Ok(RustGlobalId::Explain),
            None => Err(TryFromProtoError::missing_field("GlobalId::kind")),
        }
    }
}

impl RustType<Timestamp> for RustTimestamp {
    fn into_proto(&self) -> Timestamp {
        Timestamp {
            internal: self.into(),
        }
    }

    fn from_proto(proto: Timestamp) -> Result<Self, TryFromProtoError> {
        Ok(RustTimestamp::new(proto.internal))
    }
}

impl<T> RustType<TimestampAntichain> for Antichain<T>
where
    T: RustType<Timestamp> + Clone + timely::PartialOrder,
{
    fn into_proto(&self) -> TimestampAntichain {
        TimestampAntichain {
            elements: self
                .elements()
                .into_iter()
                .cloned()
                .map(|e| e.into_proto())
                .collect(),
        }
    }

    fn from_proto(proto: TimestampAntichain) -> Result<Self, TryFromProtoError> {
        let elements: Vec<_> = proto
            .elements
            .into_iter()
            .map(|e| T::from_proto(e))
            .collect::<Result<_, _>>()?;

        Ok(Antichain::from_iter(elements))
    }
}

impl RustType<DurableCollectionMetadata> for RustDurableCollectionMetadata {
    fn into_proto(&self) -> DurableCollectionMetadata {
        DurableCollectionMetadata {
            data_shard: self.data_shard.into_proto(),
        }
    }

    fn from_proto(proto: DurableCollectionMetadata) -> Result<Self, TryFromProtoError> {
        Ok(RustDurableCollectionMetadata {
            data_shard: proto.data_shard.into_rust()?,
        })
    }
}

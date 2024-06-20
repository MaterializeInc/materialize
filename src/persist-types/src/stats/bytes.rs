// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::fmt::Debug;

use arrow::array::BinaryArray;
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use proptest::strategy::{Strategy, Union};

use crate::columnar::Data;
use crate::dyn_struct::ValidityRef;
use crate::stats::json::{any_json_stats, JsonStats};
use crate::stats::primitive::{any_primitive_vec_u8_stats, PrimitiveStats};
use crate::stats::{
    proto_bytes_stats, proto_dyn_stats, ColumnStats, DynStats, OptionStats, ProtoAtomicBytesStats,
    ProtoBytesStats, ProtoDynStats, StatsFrom, TrimStats,
};

/// `PrimitiveStats<Vec<u8>>` that cannot safely be trimmed.
#[derive(Debug)]
#[cfg_attr(any(test), derive(Clone))]
pub struct AtomicBytesStats {
    /// See [PrimitiveStats::lower]
    pub lower: Vec<u8>,
    /// See [PrimitiveStats::upper]
    pub upper: Vec<u8>,
}

impl AtomicBytesStats {
    fn debug_json(&self) -> serde_json::Value {
        serde_json::json!({
            "lower": hex::encode(&self.lower),
            "upper": hex::encode(&self.upper),
        })
    }
}

impl RustType<ProtoAtomicBytesStats> for AtomicBytesStats {
    fn into_proto(&self) -> ProtoAtomicBytesStats {
        ProtoAtomicBytesStats {
            lower: self.lower.into_proto(),
            upper: self.upper.into_proto(),
        }
    }

    fn from_proto(proto: ProtoAtomicBytesStats) -> Result<Self, TryFromProtoError> {
        Ok(AtomicBytesStats {
            lower: proto.lower.into_rust()?,
            upper: proto.upper.into_rust()?,
        })
    }
}

/// Statistics about a column of `Vec<u8>`.
pub enum BytesStats {
    Primitive(PrimitiveStats<Vec<u8>>),
    Json(JsonStats),
    Atomic(AtomicBytesStats),
}

impl Debug for BytesStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.debug_json(), f)
    }
}

impl DynStats for BytesStats {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_proto(&self) -> ProtoDynStats {
        ProtoDynStats {
            option: None,
            kind: Some(proto_dyn_stats::Kind::Bytes(RustType::into_proto(self))),
        }
    }

    fn debug_json(&self) -> serde_json::Value {
        match self {
            BytesStats::Primitive(x) => x.debug_json(),
            BytesStats::Json(x) => x.debug_json(),
            BytesStats::Atomic(x) => x.debug_json(),
        }
    }
}

impl ColumnStats<Vec<u8>> for BytesStats {
    fn lower<'a>(&'a self) -> Option<<Vec<u8> as Data>::Ref<'a>> {
        match self {
            BytesStats::Primitive(x) => x.lower(),
            BytesStats::Json(_) => None,
            BytesStats::Atomic(x) => Some(&x.lower),
        }
    }
    fn upper<'a>(&'a self) -> Option<<Vec<u8> as Data>::Ref<'a>> {
        match self {
            BytesStats::Primitive(x) => x.upper(),
            BytesStats::Json(_) => None,
            BytesStats::Atomic(x) => Some(&x.upper),
        }
    }
    fn none_count(&self) -> usize {
        0
    }
}

impl ColumnStats<Option<Vec<u8>>> for OptionStats<BytesStats> {
    fn lower<'a>(&'a self) -> Option<<Option<Vec<u8>> as Data>::Ref<'a>> {
        self.some.lower().map(Some)
    }
    fn upper<'a>(&'a self) -> Option<<Option<Vec<u8>> as Data>::Ref<'a>> {
        self.some.upper().map(Some)
    }
    fn none_count(&self) -> usize {
        self.none
    }
}

impl StatsFrom<BinaryArray> for BytesStats {
    fn stats_from(col: &BinaryArray, validity: ValidityRef) -> Self {
        BytesStats::Primitive(<PrimitiveStats<Vec<u8>>>::stats_from(col, validity))
    }
}

impl StatsFrom<BinaryArray> for OptionStats<BytesStats> {
    fn stats_from(col: &BinaryArray, validity: ValidityRef) -> Self {
        let stats = OptionStats::<PrimitiveStats<Vec<u8>>>::stats_from(col, validity);
        OptionStats {
            none: stats.none,
            some: BytesStats::Primitive(stats.some),
        }
    }
}

impl RustType<ProtoBytesStats> for BytesStats {
    fn into_proto(&self) -> ProtoBytesStats {
        let kind = match self {
            BytesStats::Primitive(x) => proto_bytes_stats::Kind::Primitive(RustType::into_proto(x)),
            BytesStats::Json(x) => proto_bytes_stats::Kind::Json(RustType::into_proto(x)),
            BytesStats::Atomic(x) => proto_bytes_stats::Kind::Atomic(RustType::into_proto(x)),
        };
        ProtoBytesStats { kind: Some(kind) }
    }

    fn from_proto(proto: ProtoBytesStats) -> Result<Self, TryFromProtoError> {
        match proto.kind {
            Some(proto_bytes_stats::Kind::Primitive(x)) => Ok(BytesStats::Primitive(
                PrimitiveStats::<Vec<u8>>::from_proto(x)?,
            )),
            Some(proto_bytes_stats::Kind::Json(x)) => {
                Ok(BytesStats::Json(JsonStats::from_proto(x)?))
            }
            Some(proto_bytes_stats::Kind::Atomic(x)) => {
                Ok(BytesStats::Atomic(AtomicBytesStats::from_proto(x)?))
            }
            None => Err(TryFromProtoError::missing_field("ProtoBytesStats::kind")),
        }
    }
}

impl TrimStats for ProtoBytesStats {
    fn trim(&mut self) {
        use proto_bytes_stats::*;
        match &mut self.kind {
            Some(Kind::Primitive(stats)) => stats.trim(),
            Some(Kind::Json(stats)) => stats.trim(),
            // We explicitly don't trim atomic stats!
            Some(Kind::Atomic(_)) => {}
            None => {}
        }
    }
}

/// Returns a [`Strategy`] for generating arbitrary [`ByteStats`].
pub(crate) fn any_bytes_stats() -> impl Strategy<Value = BytesStats> {
    Union::new(vec![
        any_primitive_vec_u8_stats()
            .prop_map(BytesStats::Primitive)
            .boxed(),
        any_json_stats().prop_map(BytesStats::Json).boxed(),
        any_primitive_vec_u8_stats()
            .prop_map(|x| {
                BytesStats::Atomic(AtomicBytesStats {
                    lower: x.lower,
                    upper: x.upper,
                })
            })
            .boxed(),
    ])
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;

use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use proptest::strategy::{Just, Strategy, Union};
use serde::Serialize;

use crate::stats::json::{any_json_stats, JsonStats};
use crate::stats::primitive::{any_primitive_vec_u8_stats, PrimitiveStats};
use crate::stats::{
    proto_bytes_stats, proto_fixed_size_bytes_stats, ColumnStatKinds, ColumnStats, ColumnarStats,
    DynStats, OptionStats, ProtoAtomicBytesStats, ProtoBytesStats, ProtoFixedSizeBytesStats,
    TrimStats,
};

/// `PrimitiveStats<Vec<u8>>` that cannot safely be trimmed.
#[derive(Debug, Clone)]
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

/// `PrimitiveStats<Vec<u8>>` for types that implement [`FixedSizeCodec`] and
/// cannot safely be trimmed.
///
/// [`FixedSizeCodec`]: crate::columnar::FixedSizeCodec
#[derive(Debug, Clone)]
pub struct FixedSizeBytesStats {
    /// See [PrimitiveStats::lower]
    pub lower: Vec<u8>,
    /// See [PrimitiveStats::upper]
    pub upper: Vec<u8>,
    /// The kind of data these stats represent.
    pub kind: FixedSizeBytesStatsKind,
}

impl FixedSizeBytesStats {
    fn debug_json(&self) -> serde_json::Value {
        serde_json::json!({
            "lower": hex::encode(&self.lower),
            "upper": hex::encode(&self.upper),
            "kind": self.kind,
        })
    }
}

impl RustType<ProtoFixedSizeBytesStats> for FixedSizeBytesStats {
    fn into_proto(&self) -> ProtoFixedSizeBytesStats {
        ProtoFixedSizeBytesStats {
            lower: self.lower.into_proto(),
            upper: self.upper.into_proto(),
            kind: Some(self.kind.into_proto()),
        }
    }

    fn from_proto(proto: ProtoFixedSizeBytesStats) -> Result<Self, TryFromProtoError> {
        Ok(FixedSizeBytesStats {
            lower: proto.lower.into_rust()?,
            upper: proto.upper.into_rust()?,
            kind: proto
                .kind
                .into_rust_if_some("missing field ProtoFixedSizeBytesStats::kind")?,
        })
    }
}

/// The type of data encoded in an [`FixedSizeBytesStats`].
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FixedSizeBytesStatsKind {
    PackedTime,
    PackedDateTime,
    PackedInterval,
    PackedNumeric,
    Uuid,
}

impl RustType<proto_fixed_size_bytes_stats::Kind> for FixedSizeBytesStatsKind {
    fn into_proto(&self) -> proto_fixed_size_bytes_stats::Kind {
        match self {
            FixedSizeBytesStatsKind::PackedTime => {
                proto_fixed_size_bytes_stats::Kind::PackedTime(())
            }
            FixedSizeBytesStatsKind::PackedDateTime => {
                proto_fixed_size_bytes_stats::Kind::PackedDateTime(())
            }
            FixedSizeBytesStatsKind::PackedInterval => {
                proto_fixed_size_bytes_stats::Kind::PackedInterval(())
            }
            FixedSizeBytesStatsKind::PackedNumeric => {
                proto_fixed_size_bytes_stats::Kind::PackedNumeric(())
            }
            FixedSizeBytesStatsKind::Uuid => proto_fixed_size_bytes_stats::Kind::Uuid(()),
        }
    }

    fn from_proto(proto: proto_fixed_size_bytes_stats::Kind) -> Result<Self, TryFromProtoError> {
        let kind = match proto {
            proto_fixed_size_bytes_stats::Kind::PackedTime(_) => {
                FixedSizeBytesStatsKind::PackedTime
            }
            proto_fixed_size_bytes_stats::Kind::PackedDateTime(_) => {
                FixedSizeBytesStatsKind::PackedDateTime
            }
            proto_fixed_size_bytes_stats::Kind::PackedInterval(_) => {
                FixedSizeBytesStatsKind::PackedInterval
            }
            proto_fixed_size_bytes_stats::Kind::PackedNumeric(_) => {
                FixedSizeBytesStatsKind::PackedNumeric
            }
            proto_fixed_size_bytes_stats::Kind::Uuid(_) => FixedSizeBytesStatsKind::Uuid,
        };
        Ok(kind)
    }
}

/// Statistics about a column of `Vec<u8>`.
#[derive(Clone)]
pub enum BytesStats {
    Primitive(PrimitiveStats<Vec<u8>>),
    Json(JsonStats),
    Atomic(AtomicBytesStats),
    FixedSize(FixedSizeBytesStats),
}

impl Debug for BytesStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.debug_json(), f)
    }
}

impl DynStats for BytesStats {
    fn debug_json(&self) -> serde_json::Value {
        match self {
            BytesStats::Primitive(x) => x.debug_json(),
            BytesStats::Json(x) => x.debug_json(),
            BytesStats::Atomic(x) => x.debug_json(),
            BytesStats::FixedSize(x) => x.debug_json(),
        }
    }

    fn into_columnar_stats(self) -> ColumnarStats {
        ColumnarStats {
            nulls: None,
            values: ColumnStatKinds::Bytes(self),
        }
    }
}

impl ColumnStats for BytesStats {
    type Ref<'a> = &'a [u8];

    fn lower<'a>(&'a self) -> Option<Self::Ref<'a>> {
        match self {
            BytesStats::Primitive(x) => Some(x.lower.as_slice()),
            BytesStats::Json(_) => None,
            BytesStats::Atomic(x) => Some(&x.lower),
            BytesStats::FixedSize(x) => Some(&x.lower),
        }
    }
    fn upper<'a>(&'a self) -> Option<Self::Ref<'a>> {
        match self {
            BytesStats::Primitive(x) => Some(x.upper.as_slice()),
            BytesStats::Json(_) => None,
            BytesStats::Atomic(x) => Some(&x.upper),
            BytesStats::FixedSize(x) => Some(&x.upper),
        }
    }
    fn none_count(&self) -> usize {
        0
    }
}

impl ColumnStats for OptionStats<BytesStats> {
    type Ref<'a> = Option<&'a [u8]>;

    fn lower<'a>(&'a self) -> Option<Self::Ref<'a>> {
        self.some.lower().map(Some)
    }
    fn upper<'a>(&'a self) -> Option<Self::Ref<'a>> {
        self.some.upper().map(Some)
    }
    fn none_count(&self) -> usize {
        self.none
    }
}

impl RustType<ProtoBytesStats> for BytesStats {
    fn into_proto(&self) -> ProtoBytesStats {
        let kind = match self {
            BytesStats::Primitive(x) => proto_bytes_stats::Kind::Primitive(RustType::into_proto(x)),
            BytesStats::Json(x) => proto_bytes_stats::Kind::Json(RustType::into_proto(x)),
            BytesStats::Atomic(x) => proto_bytes_stats::Kind::Atomic(RustType::into_proto(x)),
            BytesStats::FixedSize(x) => proto_bytes_stats::Kind::FixedSize(RustType::into_proto(x)),
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
            Some(proto_bytes_stats::Kind::FixedSize(x)) => {
                Ok(BytesStats::FixedSize(FixedSizeBytesStats::from_proto(x)?))
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
            // We explicitly don't trim fixed size stats!
            Some(Kind::FixedSize(_)) => {}
            None => {}
        }
    }
}

/// Returns a [`Strategy`] for generating arbitrary [`BytesStats`].
pub(crate) fn any_bytes_stats() -> impl Strategy<Value = BytesStats> {
    let kind_of_packed = Union::new(vec![
        Just(FixedSizeBytesStatsKind::PackedTime),
        Just(FixedSizeBytesStatsKind::PackedInterval),
        Just(FixedSizeBytesStatsKind::PackedNumeric),
        Just(FixedSizeBytesStatsKind::Uuid),
    ]);

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
        (any_primitive_vec_u8_stats(), kind_of_packed)
            .prop_map(|(x, kind)| {
                BytesStats::FixedSize(FixedSizeBytesStats {
                    lower: x.lower,
                    upper: x.upper,
                    kind,
                })
            })
            .boxed(),
    ])
}

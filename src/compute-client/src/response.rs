// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Tonic generates code that calls clone on an Arc. Allow this here.
// TODO: Remove this once tonic does not produce this code anymore.
#![allow(clippy::clone_on_ref_ptr)]

//! Compute layer responses.

use std::num::NonZeroUsize;

use proptest::prelude::{any, Arbitrary, Just};
use proptest::prop_oneof;
use proptest::strategy::{BoxedStrategy, Strategy};
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use uuid::Uuid;

use mz_ore::tracing::OpenTelemetryContext;
use mz_proto::{any_uuid, IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{Diff, GlobalId, Row};
use mz_timely_util::progress::any_antichain;

include!(concat!(env!("OUT_DIR"), "/mz_compute_client.response.rs"));

/// Responses that the compute nature of a worker/dataflow can provide back to the coordinator.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ComputeResponse<T = mz_repr::Timestamp> {
    /// A list of identifiers of traces, with new upper frontiers.
    ///
    /// TODO(teskje): Consider also reporting the previous upper frontier and using that
    /// information to assert the correct implementation of our protocols at various places.
    FrontierUppers(Vec<(GlobalId, Antichain<T>)>),
    /// The worker's response to a specified (by connection id) peek.
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),
    /// The worker's next response to a specified tail.
    TailResponse(GlobalId, TailResponse<T>),
}

impl RustType<ProtoComputeResponse> for ComputeResponse<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoComputeResponse {
        use proto_compute_response::Kind::*;
        use proto_compute_response::*;
        ProtoComputeResponse {
            kind: Some(match self {
                ComputeResponse::FrontierUppers(traces) => FrontierUppers(traces.into_proto()),
                ComputeResponse::PeekResponse(id, resp, otel_ctx) => {
                    PeekResponse(ProtoPeekResponseKind {
                        id: Some(id.into_proto()),
                        resp: Some(resp.into_proto()),
                        otel_ctx: otel_ctx.clone().into(),
                    })
                }
                ComputeResponse::TailResponse(id, resp) => TailResponse(ProtoTailResponseKind {
                    id: Some(id.into_proto()),
                    resp: Some(resp.into_proto()),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoComputeResponse) -> Result<Self, TryFromProtoError> {
        use proto_compute_response::Kind::*;
        match proto.kind {
            Some(FrontierUppers(traces)) => {
                Ok(ComputeResponse::FrontierUppers(traces.into_rust()?))
            }
            Some(PeekResponse(resp)) => Ok(ComputeResponse::PeekResponse(
                resp.id.into_rust_if_some("ProtoPeekResponseKind::id")?,
                resp.resp.into_rust_if_some("ProtoPeekResponseKind::resp")?,
                resp.otel_ctx.into(),
            )),
            Some(TailResponse(resp)) => Ok(ComputeResponse::TailResponse(
                resp.id.into_rust_if_some("ProtoTailResponseKind::id")?,
                resp.resp.into_rust_if_some("ProtoTailResponseKind::resp")?,
            )),
            None => Err(TryFromProtoError::missing_field(
                "ProtoComputeResponse::kind",
            )),
        }
    }
}

impl Arbitrary for ComputeResponse<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            proptest::collection::vec((any::<GlobalId>(), any_antichain()), 1..4)
                .prop_map(ComputeResponse::FrontierUppers),
            (any_uuid(), any::<PeekResponse>()).prop_map(|(id, resp)| {
                ComputeResponse::PeekResponse(id, resp, OpenTelemetryContext::empty())
            }),
            (any::<GlobalId>(), any::<TailResponse>())
                .prop_map(|(id, resp)| ComputeResponse::TailResponse(id, resp)),
        ]
        .boxed()
    }
}

/// The response from a `Peek`.
///
/// Note that each `Peek` expects to generate exactly one `PeekResponse`, i.e.
/// we expect a 1:1 contract between `Peek` and `PeekResponse`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum PeekResponse {
    Rows(Vec<(Row, NonZeroUsize)>),
    Error(String),
    Canceled,
}

impl PeekResponse {
    pub fn unwrap_rows(self) -> Vec<(Row, NonZeroUsize)> {
        match self {
            PeekResponse::Rows(rows) => rows,
            PeekResponse::Error(_) | PeekResponse::Canceled => {
                panic!("PeekResponse::unwrap_rows called on {:?}", self)
            }
        }
    }
}

impl RustType<ProtoPeekResponse> for PeekResponse {
    fn into_proto(&self) -> ProtoPeekResponse {
        use proto_peek_response::Kind::*;
        use proto_peek_response::*;
        ProtoPeekResponse {
            kind: Some(match self {
                PeekResponse::Rows(rows) => Rows(ProtoRows {
                    rows: rows
                        .iter()
                        .map(|(r, d)| ProtoRow {
                            row: Some(r.into_proto()),
                            diff: d.into_proto(),
                        })
                        .collect(),
                }),
                PeekResponse::Error(err) => proto_peek_response::Kind::Error(err.clone()),
                PeekResponse::Canceled => Canceled(()),
            }),
        }
    }

    fn from_proto(proto: ProtoPeekResponse) -> Result<Self, TryFromProtoError> {
        use proto_peek_response::Kind::*;
        match proto.kind {
            Some(Rows(rows)) => Ok(PeekResponse::Rows(
                rows.rows
                    .into_iter()
                    .map(|row| {
                        Ok((
                            row.row.into_rust_if_some("ProtoRow::row")?,
                            NonZeroUsize::from_proto(row.diff)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, TryFromProtoError>>()?,
            )),
            Some(proto_peek_response::Kind::Error(err)) => Ok(PeekResponse::Error(err)),
            Some(Canceled(())) => Ok(PeekResponse::Canceled),
            None => Err(TryFromProtoError::missing_field("ProtoPeekResponse::kind")),
        }
    }
}

impl Arbitrary for PeekResponse {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            proptest::collection::vec(
                (
                    any::<Row>(),
                    (1..usize::MAX).prop_map(|u| NonZeroUsize::try_from(u).unwrap())
                ),
                1..11
            )
            .prop_map(PeekResponse::Rows),
            ".*".prop_map(PeekResponse::Error),
            Just(PeekResponse::Canceled),
        ]
        .boxed()
    }
}

/// Various responses that can be communicated about the progress of a TAIL command.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum TailResponse<T = mz_repr::Timestamp> {
    /// A batch of updates over a non-empty interval of time.
    Batch(TailBatch<T>),
    /// The TAIL dataflow was dropped, leaving updates from this frontier onward unspecified.
    DroppedAt(Antichain<T>),
}

impl RustType<ProtoTailResponse> for TailResponse<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoTailResponse {
        use proto_tail_response::Kind::*;
        ProtoTailResponse {
            kind: Some(match self {
                TailResponse::Batch(tail_batch) => Batch(tail_batch.into_proto()),
                TailResponse::DroppedAt(antichain) => DroppedAt(antichain.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoTailResponse) -> Result<Self, TryFromProtoError> {
        use proto_tail_response::Kind::*;
        match proto.kind {
            Some(Batch(tail_batch)) => Ok(TailResponse::Batch(tail_batch.into_rust()?)),
            Some(DroppedAt(antichain)) => Ok(TailResponse::DroppedAt(antichain.into_rust()?)),
            None => Err(TryFromProtoError::missing_field("ProtoTailResponse::kind")),
        }
    }
}

impl Arbitrary for TailResponse<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any::<TailBatch<mz_repr::Timestamp>>().prop_map(TailResponse::Batch),
            proptest::collection::vec(any::<u64>(), 1..4)
                .prop_map(|antichain| TailResponse::DroppedAt(Antichain::from(antichain)))
        ]
        .boxed()
    }
}

/// A batch of updates for the interval `[lower, upper)`.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TailBatch<T> {
    /// The lower frontier of the batch of updates.
    pub lower: Antichain<T>,
    /// The upper frontier of the batch of updates.
    pub upper: Antichain<T>,
    /// All updates greater than `lower` and not greater than `upper`.
    pub updates: Vec<(T, Row, Diff)>,
}

impl RustType<ProtoTailBatch> for TailBatch<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoTailBatch {
        use proto_tail_batch::ProtoUpdate;
        ProtoTailBatch {
            lower: Some(self.lower.into_proto()),
            upper: Some(self.upper.into_proto()),
            updates: self
                .updates
                .iter()
                .map(|(t, r, d)| ProtoUpdate {
                    timestamp: *t,
                    row: Some(r.into_proto()),
                    diff: *d,
                })
                .collect(),
        }
    }

    fn from_proto(proto: ProtoTailBatch) -> Result<Self, TryFromProtoError> {
        Ok(TailBatch {
            lower: proto.lower.into_rust_if_some("ProtoTailUpdate::lower")?,
            upper: proto.upper.into_rust_if_some("ProtoTailUpdate::upper")?,
            updates: proto
                .updates
                .into_iter()
                .map(|update| {
                    Ok((
                        update.timestamp,
                        update.row.into_rust_if_some("ProtoUpdate::row")?,
                        update.diff,
                    ))
                })
                .collect::<Result<Vec<_>, TryFromProtoError>>()?,
        })
    }
}

impl Arbitrary for TailBatch<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            proptest::collection::vec(any::<u64>(), 1..4),
            proptest::collection::vec(any::<u64>(), 1..4),
            proptest::collection::vec(
                (any::<mz_repr::Timestamp>(), any::<Row>(), any::<Diff>()),
                1..4,
            ),
        )
            .prop_map(|(lower, upper, updates)| TailBatch {
                lower: Antichain::from(lower),
                upper: Antichain::from(upper),
                updates,
            })
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::ProptestConfig;
    use proptest::proptest;

    use mz_proto::protobuf_roundtrip;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn compute_response_protobuf_roundtrip(expect in any::<ComputeResponse<mz_repr::Timestamp>>() ) {
            let actual = protobuf_roundtrip::<_, ProtoComputeResponse>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}

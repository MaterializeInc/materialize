// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compute protocol responses.

use std::num::NonZeroUsize;

use mz_ore::tracing::OpenTelemetryContext;
use mz_proto::{any_uuid, IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{Diff, GlobalId, Row};
use mz_timely_util::progress::any_antichain;
use proptest::prelude::{any, Arbitrary, Just};
use proptest::strategy::{BoxedStrategy, Strategy, Union};
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use uuid::Uuid;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_compute_client.protocol.response.rs"
));

/// Compute protocol responses, sent by replicas to the compute controller.
///
/// Replicas send `ComputeResponse`s in response to [`ComputeCommand`]s they previously received
/// from the compute controller.
///
/// [`ComputeCommand`]: super::command::ComputeCommand
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ComputeResponse<T = mz_repr::Timestamp> {
    /// `FrontierUppers` announces the advancement of the upper frontiers of the specified compute
    /// collections. The response contains a mapping of collection IDs to their new upper
    /// frontiers.
    ///
    /// Upon receiving a `FrontierUppers` response, the controller may assume that the replica has
    /// finished computing the given collections up to at least the given frontiers. It may also
    /// assume that the replica has finished reading from the collections’ inputs up to those
    /// frontiers.
    ///
    /// Replicas must send `FrontierUppers` responses for compute collections that are indexes or
    /// storage sinks. Replicas must not send `FrontierUppers` responses for subscribes.
    ///
    /// Replicas must never report regressing frontiers. Specifically:
    ///
    ///   * The first frontier reported for a collection must not be less than that collection's
    ///     initial `as_of` frontier.
    ///   * Subsequent reported frontiers for a collection must not be less than any frontier
    ///     reported previously for the same collection.
    ///
    /// Replicas must send a `FrontierUppers` response reporting advancement to the empty frontier
    /// for a collection in two cases:
    ///
    ///   * The collection has advanced to the empty frontier (e.g. because its inputs have advanced
    ///     to the empty frontier).
    ///   * The collection was dropped in response to an [`AllowCompaction` command] that advanced
    ///     its read frontier to the empty frontier. ([#16275])
    ///
    /// Once a collection was reported to have been advanced to the empty upper frontier:
    ///
    ///   * It must no longer read from its inputs.
    ///   * The replica must not send further `FrontierUppers` responses for that collection.
    ///
    /// The replica must not send `FrontierUppers` responses for collections that have not
    /// been created previously by a [`CreateDataflows` command] or by a [`CreateInstance`
    /// command].
    ///
    /// [`AllowCompaction` command]: super::command::ComputeCommand::AllowCompaction
    /// [`CreateDataflows` command]: super::command::ComputeCommand::CreateDataflows
    /// [`CreateInstance` command]: super::command::ComputeCommand::CreateInstance
    /// [#16275]: https://github.com/MaterializeInc/materialize/issues/16275
    FrontierUppers(Vec<(GlobalId, Antichain<T>)>),

    /// `PeekResponse` reports the result of a previous [`Peek` command]. The peek is identified by
    /// a `Uuid` that matches the command's [`Peek::uuid`].
    ///
    /// The replica must send exactly one `PeekResponse` for every [`Peek` command] it received.
    ///
    /// If the replica did not receive a [`CancelPeeks` command] for a peek, it must not send a
    /// [`Canceled`] response for that peek. If the replica did receive a [`CancelPeeks` command]
    /// for a peek, it may send any of the three [`PeekResponse`] variants.
    ///
    /// The replica must not send `PeekResponse`s for peek IDs that were not previously specified
    /// in a [`Peek` command].
    ///
    /// [`Peek` command]: super::command::ComputeCommand::Peek
    /// [`CancelPeeks` command]: super::command::ComputeCommand::CancelPeeks
    /// [`Peek::uuid`]: super::command::Peek::uuid
    /// [`Canceled`]: PeekResponse::Canceled
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),

    /// `SubscribeResponse` reports the results emitted by an active subscribe over some time
    /// interval.
    ///
    /// For each subscribe that was installed by a previous [`CreateDataflows` command], the
    /// replica must emit [`Batch`] responses that cover the entire time interval from the
    /// minimum time until the subscribe advances to the empty frontier or is
    /// dropped. The time intervals of consecutive [`Batch`]es must be increasing, contiguous,
    /// non-overlapping, and non-empty. All updates transmitted in a batch must be consolidated and
    /// have times within that batch’s time interval. All updates' times must be greater than or
    /// equal to `as_of`. The `upper` of the first [`Batch`] of a subscribe must not be less than
    /// that subscribe's initial `as_of` frontier.
    ///
    /// The replica must send [`DroppedAt`] responses if the subscribe was dropped in response to
    /// an [`AllowCompaction` command] that advanced its read frontier to the empty frontier. The
    /// [`DroppedAt`] frontier must be the upper frontier of the last emitted batch.
    ///
    /// The replica must not send a [`DroppedAt`] response if the subscribe’s upper frontier
    /// (reported by [`Batch`] responses) has advanced to the empty frontier (e.g. because its
    /// inputs advanced to the empty frontier).
    ///
    /// Once a subscribe was reported to have advanced to the empty frontier, or has been dropped:
    ///
    ///   * It must no longer read from its inputs.
    ///   * The replica must not send further `SubscribeResponse`s for that subscribe.
    ///
    /// The replica must not send `SubscribeResponse`s for subscribes that have not been
    /// created previously by a [`CreateDataflows` command].
    ///
    /// [`Batch`]: SubscribeResponse::Batch
    /// [`DroppedAt`]: SubscribeResponse::DroppedAt
    /// [`CreateDataflows` command]: super::command::ComputeCommand::CreateDataflows
    /// [`AllowCompaction` command]: super::command::ComputeCommand::AllowCompaction
    SubscribeResponse(GlobalId, SubscribeResponse<T>),
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
                ComputeResponse::SubscribeResponse(id, resp) => {
                    SubscribeResponse(ProtoSubscribeResponseKind {
                        id: Some(id.into_proto()),
                        resp: Some(resp.into_proto()),
                    })
                }
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
            Some(SubscribeResponse(resp)) => Ok(ComputeResponse::SubscribeResponse(
                resp.id
                    .into_rust_if_some("ProtoSubscribeResponseKind::id")?,
                resp.resp
                    .into_rust_if_some("ProtoSubscribeResponseKind::resp")?,
            )),
            None => Err(TryFromProtoError::missing_field(
                "ProtoComputeResponse::kind",
            )),
        }
    }
}

impl Arbitrary for ComputeResponse<mz_repr::Timestamp> {
    type Strategy = Union<BoxedStrategy<Self>>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        Union::new(vec![
            proptest::collection::vec((any::<GlobalId>(), any_antichain()), 1..4)
                .prop_map(ComputeResponse::FrontierUppers)
                .boxed(),
            (any_uuid(), any::<PeekResponse>())
                .prop_map(|(id, resp)| {
                    ComputeResponse::PeekResponse(id, resp, OpenTelemetryContext::empty())
                })
                .boxed(),
            (any::<GlobalId>(), any::<SubscribeResponse>())
                .prop_map(|(id, resp)| ComputeResponse::SubscribeResponse(id, resp))
                .boxed(),
        ])
    }
}

/// The response from a `Peek`.
///
/// Note that each `Peek` expects to generate exactly one `PeekResponse`, i.e.
/// we expect a 1:1 contract between `Peek` and `PeekResponse`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum PeekResponse {
    /// Returned rows of a successful peek.
    Rows(Vec<(Row, NonZeroUsize)>),
    /// Error of an unsuccessful peek.
    Error(String),
    /// The peek was canceled.
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
    type Strategy = Union<BoxedStrategy<Self>>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        Union::new(vec![
            proptest::collection::vec(
                (
                    any::<Row>(),
                    (1..usize::MAX).prop_map(|u| NonZeroUsize::try_from(u).unwrap()),
                ),
                1..11,
            )
            .prop_map(PeekResponse::Rows)
            .boxed(),
            ".*".prop_map(PeekResponse::Error).boxed(),
            Just(PeekResponse::Canceled).boxed(),
        ])
    }
}

/// Various responses that can be communicated about the progress of a SUBSCRIBE command.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum SubscribeResponse<T = mz_repr::Timestamp> {
    /// A batch of updates over a non-empty interval of time.
    Batch(SubscribeBatch<T>),
    /// The SUBSCRIBE dataflow was dropped, leaving updates from this frontier onward unspecified.
    DroppedAt(Antichain<T>),
}

impl<T> SubscribeResponse<T> {
    /// Converts `self` to an error if a maximum size is exceeded.
    pub fn to_error_if_exceeds(&mut self, max_result_size: usize) {
        if let SubscribeResponse::Batch(batch) = self {
            batch.to_error_if_exceeds(max_result_size);
        }
    }
}

impl RustType<ProtoSubscribeResponse> for SubscribeResponse<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoSubscribeResponse {
        use proto_subscribe_response::Kind::*;
        ProtoSubscribeResponse {
            kind: Some(match self {
                SubscribeResponse::Batch(subscribe_batch) => Batch(subscribe_batch.into_proto()),
                SubscribeResponse::DroppedAt(antichain) => DroppedAt(antichain.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoSubscribeResponse) -> Result<Self, TryFromProtoError> {
        use proto_subscribe_response::Kind::*;
        match proto.kind {
            Some(Batch(subscribe_batch)) => {
                Ok(SubscribeResponse::Batch(subscribe_batch.into_rust()?))
            }
            Some(DroppedAt(antichain)) => Ok(SubscribeResponse::DroppedAt(antichain.into_rust()?)),
            None => Err(TryFromProtoError::missing_field(
                "ProtoSubscribeResponse::kind",
            )),
        }
    }
}

impl Arbitrary for SubscribeResponse<mz_repr::Timestamp> {
    type Strategy = Union<BoxedStrategy<Self>>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        Union::new(vec![
            any::<SubscribeBatch<mz_repr::Timestamp>>()
                .prop_map(SubscribeResponse::Batch)
                .boxed(),
            proptest::collection::vec(any::<mz_repr::Timestamp>(), 1..4)
                .prop_map(|antichain| SubscribeResponse::DroppedAt(Antichain::from(antichain)))
                .boxed(),
        ])
    }
}

/// A batch of updates for the interval `[lower, upper)`.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubscribeBatch<T> {
    /// The lower frontier of the batch of updates.
    pub lower: Antichain<T>,
    /// The upper frontier of the batch of updates.
    pub upper: Antichain<T>,
    /// All updates greater than `lower` and not greater than `upper`.
    ///
    /// An `Err` variant can be used to indicate e.g. that the size of the updates exceeds internal limits.
    pub updates: Result<Vec<(T, Row, Diff)>, String>,
}

impl<T> SubscribeBatch<T> {
    /// Converts `self` to an error if a maximum size is exceeded.
    fn to_error_if_exceeds(&mut self, max_result_size: usize) {
        use bytesize::ByteSize;
        if let Ok(updates) = &self.updates {
            let total_size: usize = updates
                .iter()
                .map(|(_time, row, _diff)| row.byte_len())
                .sum();
            if total_size > max_result_size {
                use mz_ore::cast::CastFrom;
                self.updates = Err(format!(
                    "result exceeds max size of {}",
                    ByteSize::b(u64::cast_from(max_result_size))
                ));
            }
        }
    }
}

impl RustType<ProtoSubscribeBatch> for SubscribeBatch<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoSubscribeBatch {
        use proto_subscribe_batch::ProtoUpdate;
        ProtoSubscribeBatch {
            lower: Some(self.lower.into_proto()),
            upper: Some(self.upper.into_proto()),
            updates: Some(proto_subscribe_batch::ProtoSubscribeBatchContents {
                kind: match &self.updates {
                    Ok(updates) => {
                        let updates = updates
                            .iter()
                            .map(|(t, r, d)| ProtoUpdate {
                                timestamp: t.into(),
                                row: Some(r.into_proto()),
                                diff: *d,
                            })
                            .collect();

                        Some(
                            proto_subscribe_batch::proto_subscribe_batch_contents::Kind::Updates(
                                proto_subscribe_batch::ProtoSubscribeUpdates { updates },
                            ),
                        )
                    }
                    Err(text) => Some(
                        proto_subscribe_batch::proto_subscribe_batch_contents::Kind::Error(
                            text.clone(),
                        ),
                    ),
                },
            }),
        }
    }

    fn from_proto(proto: ProtoSubscribeBatch) -> Result<Self, TryFromProtoError> {
        Ok(SubscribeBatch {
            lower: proto.lower.into_rust_if_some("ProtoTailUpdate::lower")?,
            upper: proto.upper.into_rust_if_some("ProtoTailUpdate::upper")?,
            updates: match proto.updates.unwrap().kind {
                Some(proto_subscribe_batch::proto_subscribe_batch_contents::Kind::Updates(
                    updates,
                )) => Ok(updates
                    .updates
                    .into_iter()
                    .map(|update| {
                        Ok((
                            update.timestamp.into(),
                            update.row.into_rust_if_some("ProtoUpdate::row")?,
                            update.diff,
                        ))
                    })
                    .collect::<Result<Vec<_>, TryFromProtoError>>()?),
                Some(proto_subscribe_batch::proto_subscribe_batch_contents::Kind::Error(text)) => {
                    Err(text)
                }
                None => Err(TryFromProtoError::missing_field("ProtoPeekResponse::kind"))?,
            },
        })
    }
}

impl Arbitrary for SubscribeBatch<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            proptest::collection::vec(any::<mz_repr::Timestamp>(), 1..4),
            proptest::collection::vec(any::<mz_repr::Timestamp>(), 1..4),
            proptest::collection::vec(
                (any::<mz_repr::Timestamp>(), any::<Row>(), any::<Diff>()),
                1..4,
            ),
        )
            .prop_map(|(lower, upper, updates)| SubscribeBatch {
                lower: Antichain::from(lower),
                upper: Antichain::from(upper),
                updates: Ok(updates),
            })
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::ProptestConfig;
    use proptest::proptest;

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

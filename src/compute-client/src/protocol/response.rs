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

use mz_expr::row::RowCollection;
use mz_ore::cast::CastFrom;
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_client::batch::ProtoBatch;
use mz_persist_types::ShardId;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError, any_uuid};
use mz_repr::{Diff, GlobalId, RelationDesc, Row};
use mz_timely_util::progress::any_antichain;
use proptest::prelude::{Arbitrary, any};
use proptest::strategy::{BoxedStrategy, Just, Strategy, Union};
use proptest_derive::Arbitrary;
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
    /// `Frontiers` announces the advancement of the various frontiers of the specified compute
    /// collection.
    ///
    /// Replicas must send `Frontiers` responses for compute collections that are indexes or
    /// storage sinks. Replicas must not send `Frontiers` responses for subscribes and copy-tos
    /// ([#16274]).
    ///
    /// Replicas must never report regressing frontiers. Specifically:
    ///
    ///   * The first frontier of any kind reported for a collection must not be less than that
    ///     collection's initial `as_of` frontier.
    ///   * Subsequent reported frontiers for a collection must not be less than any frontier of
    ///     the same kind reported previously for the same collection.
    ///
    /// Replicas must send `Frontiers` responses that report each frontier kind to have advanced to
    /// the empty frontier in response to an [`AllowCompaction` command] that allows compaction of
    /// the collection to to the empty frontier, unless the frontier has previously advanced to the
    /// empty frontier as part of the regular dataflow computation. ([#16271])
    ///
    /// Once a frontier was reported to have been advanced to the empty frontier, the replica must
    /// not send further `Frontiers` responses with non-`None` values for that frontier kind.
    ///
    /// The replica must not send `Frontiers` responses for collections that have not
    /// been created previously by a [`CreateDataflow` command] or by a [`CreateInstance`
    /// command].
    ///
    /// [`AllowCompaction` command]: super::command::ComputeCommand::AllowCompaction
    /// [`CreateDataflow` command]: super::command::ComputeCommand::CreateDataflow
    /// [`CreateInstance` command]: super::command::ComputeCommand::CreateInstance
    /// [#16271]: https://github.com/MaterializeInc/database-issues/issues/4699
    /// [#16274]: https://github.com/MaterializeInc/database-issues/issues/4701
    Frontiers(GlobalId, FrontiersResponse<T>),

    /// `PeekResponse` reports the result of a previous [`Peek` command]. The peek is identified by
    /// a `Uuid` that matches the command's [`Peek::uuid`].
    ///
    /// The replica must send exactly one `PeekResponse` for every [`Peek` command] it received.
    ///
    /// If the replica did not receive a [`CancelPeek` command] for a peek, it must not send a
    /// [`Canceled`] response for that peek. If the replica did receive a [`CancelPeek` command]
    /// for a peek, it may send any of the three [`PeekResponse`] variants.
    ///
    /// The replica must not send `PeekResponse`s for peek IDs that were not previously specified
    /// in a [`Peek` command].
    ///
    /// [`Peek` command]: super::command::ComputeCommand::Peek
    /// [`CancelPeek` command]: super::command::ComputeCommand::CancelPeek
    /// [`Peek::uuid`]: super::command::Peek::uuid
    /// [`Canceled`]: PeekResponse::Canceled
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),

    /// `SubscribeResponse` reports the results emitted by an active subscribe over some time
    /// interval.
    ///
    /// For each subscribe that was installed by a previous [`CreateDataflow` command], the
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
    /// created previously by a [`CreateDataflow` command].
    ///
    /// [`Batch`]: SubscribeResponse::Batch
    /// [`DroppedAt`]: SubscribeResponse::DroppedAt
    /// [`CreateDataflow` command]: super::command::ComputeCommand::CreateDataflow
    /// [`AllowCompaction` command]: super::command::ComputeCommand::AllowCompaction
    SubscribeResponse(GlobalId, SubscribeResponse<T>),

    /// `CopyToResponse` reports the completion of an S3-oneshot sink.
    ///
    /// The replica must send exactly one `CopyToResponse` for every S3-oneshot sink previously
    /// created by a [`CreateDataflow` command].
    ///
    /// The replica must not send `CopyToResponse`s for S3-oneshot sinks that were not previously
    /// created by a [`CreateDataflow` command].
    ///
    /// [`CreateDataflow` command]: super::command::ComputeCommand::CreateDataflow
    CopyToResponse(GlobalId, CopyToResponse),

    /// `Status` reports status updates from replicas to the controller.
    ///
    /// `Status` responses are a way for replicas to stream back introspection data that the
    /// controller can then announce to its clients. They have no effect on the lifecycles of
    /// compute collections. Correct operation of the Compute layer must not rely on `Status`
    /// responses being sent or received.
    ///
    /// `Status` responses that are specific to collections must only be sent for collections that
    /// (a) have previously been created by a [`CreateDataflow` command] and (b) have not yet
    /// been reported to have advanced to the empty frontier.
    ///
    /// [`CreateDataflow` command]: super::command::ComputeCommand::CreateDataflow
    Status(StatusResponse),
}

impl RustType<ProtoComputeResponse> for ComputeResponse<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoComputeResponse {
        use proto_compute_response::Kind::*;
        use proto_compute_response::*;
        ProtoComputeResponse {
            kind: Some(match self {
                ComputeResponse::Frontiers(id, resp) => Frontiers(ProtoFrontiersKind {
                    id: Some(id.into_proto()),
                    resp: Some(resp.into_proto()),
                }),
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
                ComputeResponse::CopyToResponse(id, resp) => {
                    CopyToResponse(ProtoCopyToResponseKind {
                        id: Some(id.into_proto()),
                        resp: Some(resp.into_proto()),
                    })
                }
                ComputeResponse::Status(resp) => Status(resp.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoComputeResponse) -> Result<Self, TryFromProtoError> {
        use proto_compute_response::Kind::*;
        match proto.kind {
            Some(Frontiers(resp)) => Ok(ComputeResponse::Frontiers(
                resp.id.into_rust_if_some("ProtoFrontiersKind::id")?,
                resp.resp.into_rust_if_some("ProtoFrontiersKind::resp")?,
            )),
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
            Some(CopyToResponse(resp)) => Ok(ComputeResponse::CopyToResponse(
                resp.id.into_rust_if_some("ProtoCopyToResponseKind::id")?,
                resp.resp
                    .into_rust_if_some("ProtoCopyToResponseKind::resp")?,
            )),
            Some(Status(resp)) => Ok(ComputeResponse::Status(resp.into_rust()?)),
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
            (any::<GlobalId>(), any::<FrontiersResponse>())
                .prop_map(|(id, resp)| ComputeResponse::Frontiers(id, resp))
                .boxed(),
            (any_uuid(), any::<PeekResponse>())
                .prop_map(|(id, resp)| {
                    ComputeResponse::PeekResponse(id, resp, OpenTelemetryContext::empty())
                })
                .boxed(),
            (any::<GlobalId>(), any::<SubscribeResponse>())
                .prop_map(|(id, resp)| ComputeResponse::SubscribeResponse(id, resp))
                .boxed(),
            any::<StatusResponse>()
                .prop_map(ComputeResponse::Status)
                .boxed(),
        ])
    }
}

/// A response reporting advancement of frontiers of a compute collection.
///
/// All contained frontier fields are optional. `None` values imply that the respective frontier
/// has not advanced and the previously reported value is still current.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FrontiersResponse<T = mz_repr::Timestamp> {
    /// The collection's new write frontier, if any.
    ///
    /// Upon receiving an updated `write_frontier`, the controller may assume that the contents of the
    /// collection are sealed for all times less than that frontier. Once it has reported the
    /// `write_frontier` as the empty frontier, the replica must no longer change the contents of the
    /// collection.
    pub write_frontier: Option<Antichain<T>>,
    /// The collection's new input frontier, if any.
    ///
    /// Upon receiving an updated `input_frontier`, the controller may assume that the replica has
    /// finished reading from the collection’s inputs up to that frontier. Once it has reported the
    /// `input_frontier` as the empty frontier, the replica must no longer read from the
    /// collection's inputs.
    pub input_frontier: Option<Antichain<T>>,
    /// The collection's new output frontier, if any.
    ///
    /// Upon receiving an updated `output_frontier`, the controller may assume that the replica
    /// has finished processing the collection's input up to that frontier.
    ///
    /// The `output_frontier` is often equal to the `write_frontier`, but not always. Some
    /// collections can jump their write frontiers ahead of the times they have finished
    /// processing, causing the `output_frontier` to lag behind the `write_frontier`. Collections
    /// writing materialized views do so in two cases:
    ///
    ///  * `REFRESH` MVs jump their write frontier ahead to the next refresh time.
    ///  * In a multi-replica cluster, slower replicas observe and report the write frontier of the
    ///    fastest replica, by witnessing advancements of the target persist shard's `upper`.
    pub output_frontier: Option<Antichain<T>>,
}

impl<T> FrontiersResponse<T> {
    /// Returns whether there are any contained updates.
    pub fn has_updates(&self) -> bool {
        self.write_frontier.is_some()
            || self.input_frontier.is_some()
            || self.output_frontier.is_some()
    }
}

impl RustType<ProtoFrontiersResponse> for FrontiersResponse {
    fn into_proto(&self) -> ProtoFrontiersResponse {
        ProtoFrontiersResponse {
            write_frontier: self.write_frontier.into_proto(),
            input_frontier: self.input_frontier.into_proto(),
            output_frontier: self.output_frontier.into_proto(),
        }
    }

    fn from_proto(proto: ProtoFrontiersResponse) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            write_frontier: proto.write_frontier.into_rust()?,
            input_frontier: proto.input_frontier.into_rust()?,
            output_frontier: proto.output_frontier.into_rust()?,
        })
    }
}

impl Arbitrary for FrontiersResponse {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (any_antichain(), any_antichain(), any_antichain())
            .prop_map(|(write, input, compute)| Self {
                write_frontier: Some(write),
                input_frontier: Some(input),
                output_frontier: Some(compute),
            })
            .boxed()
    }
}

/// The response from a `Peek`.
///
/// Note that each `Peek` expects to generate exactly one `PeekResponse`, i.e.
/// we expect a 1:1 contract between `Peek` and `PeekResponse`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PeekResponse {
    /// Returned rows of a successful peek.
    Rows(RowCollection),
    /// Results of the peek were stashed in persist batches.
    Stashed(Box<StashedPeekResponse>),
    /// Error of an unsuccessful peek.
    Error(String),
    /// The peek was canceled.
    Canceled,
}

impl PeekResponse {
    /// Return the size of row bytes stored inline in this response.
    pub fn inline_byte_len(&self) -> usize {
        match self {
            Self::Rows(rows) => rows.byte_len(),
            Self::Stashed(stashed) => stashed.inline_rows.byte_len(),
            Self::Error(_) | Self::Canceled => 0,
        }
    }
}

impl RustType<ProtoPeekResponse> for PeekResponse {
    fn into_proto(&self) -> ProtoPeekResponse {
        use proto_peek_response::Kind::*;
        ProtoPeekResponse {
            kind: Some(match self {
                PeekResponse::Rows(rows) => Rows(rows.into_proto()),
                PeekResponse::Stashed(stashed) => Stashed(stashed.as_ref().into_proto()),
                PeekResponse::Error(err) => proto_peek_response::Kind::Error(err.clone()),
                PeekResponse::Canceled => Canceled(()),
            }),
        }
    }

    fn from_proto(proto: ProtoPeekResponse) -> Result<Self, TryFromProtoError> {
        use proto_peek_response::Kind::*;
        match proto.kind {
            Some(Rows(rows)) => Ok(PeekResponse::Rows(rows.into_rust()?)),
            Some(Stashed(stashed)) => Ok(PeekResponse::Stashed(Box::new(stashed.into_rust()?))),
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
            .prop_map(|rows| PeekResponse::Rows(RowCollection::new(rows, &[])))
            .boxed(),
            ".*".prop_map(PeekResponse::Error).boxed(),
            Just(PeekResponse::Canceled).boxed(),
        ])
    }
}

/// Response from a peek whose results have been stashed into persist.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StashedPeekResponse {
    /// The number of rows stored in response batches. This is the sum of the
    /// diff values of the contained rows.
    ///
    /// This does _NOT_ include rows in `inline_rows`.
    pub num_rows_batches: u64,
    /// The sum of the encoded sizes of all batches in this response.
    pub encoded_size_bytes: usize,
    /// [RelationDesc] for the rows in these stashed batches of results.
    pub relation_desc: RelationDesc,
    /// The [ShardId] under which result batches have been stashed.
    pub shard_id: ShardId,
    /// Batches of Rows, must be combined with reponses from other workers and
    /// consolidated before sending back via a client.
    pub batches: Vec<ProtoBatch>,
    /// Rows that have not been uploaded to the stash, because their total size
    /// did not go above the threshold for using the peek stash.
    ///
    /// We will have a mix of stashed responses and inline responses because the
    /// result sizes across different workers can and will vary.
    pub inline_rows: RowCollection,
}

impl StashedPeekResponse {
    /// Total count of [`Row`]s represented by this collection, considering a
    /// possible `OFFSET` and `LIMIT`.
    pub fn num_rows(&self, offset: usize, limit: Option<usize>) -> usize {
        let num_stashed_rows: usize = usize::cast_from(self.num_rows_batches);
        let num_inline_rows = self.inline_rows.count(offset, limit);
        let mut num_rows = num_stashed_rows + num_inline_rows;

        // Consider a possible OFFSET.
        num_rows = num_rows.saturating_sub(offset);

        // Consider a possible LIMIT.
        if let Some(limit) = limit {
            num_rows = std::cmp::min(limit, num_rows);
        }

        num_rows
    }

    /// The size in bytes of the encoded rows in this result.
    pub fn size_bytes(&self) -> usize {
        let inline_size = self.inline_rows.byte_len();

        self.encoded_size_bytes + inline_size
    }
}

impl RustType<ProtoStashedPeekResponse> for StashedPeekResponse {
    fn into_proto(&self) -> ProtoStashedPeekResponse {
        ProtoStashedPeekResponse {
            relation_desc: Some(self.relation_desc.into_proto()),
            shard_id: self.shard_id.into_proto(),
            batches: self.batches.clone(),
            num_rows: self.num_rows_batches.into_proto(),
            encoded_size_bytes: self.encoded_size_bytes.into_proto(),
            inline_rows: Some(self.inline_rows.into_proto()),
        }
    }

    fn from_proto(proto: ProtoStashedPeekResponse) -> Result<Self, TryFromProtoError> {
        let shard_id: ShardId = proto
            .shard_id
            .into_rust()
            .expect("valid transmittable shard_id");
        Ok(StashedPeekResponse {
            relation_desc: proto
                .relation_desc
                .into_rust_if_some("ProtoStashedPeekResponse::relation_desc")?,
            shard_id,
            batches: proto.batches,
            num_rows_batches: proto.num_rows,
            encoded_size_bytes: usize::cast_from(proto.encoded_size_bytes),
            inline_rows: proto
                .inline_rows
                .into_rust_if_some("ProtoStashedPeekResponse::inline_rows")?,
        })
    }
}

/// Various responses that can be communicated after a COPY TO command.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum CopyToResponse {
    /// Returned number of rows for a successful COPY TO.
    RowCount(u64),
    /// Error of an unsuccessful COPY TO.
    Error(String),
    /// The COPY TO sink dataflow was dropped.
    Dropped,
}

impl RustType<ProtoCopyToResponse> for CopyToResponse {
    fn into_proto(&self) -> ProtoCopyToResponse {
        use proto_copy_to_response::Kind::*;
        ProtoCopyToResponse {
            kind: Some(match self {
                CopyToResponse::RowCount(rows) => Rows(*rows),
                CopyToResponse::Error(error) => Error(error.clone()),
                CopyToResponse::Dropped => Dropped(()),
            }),
        }
    }

    fn from_proto(proto: ProtoCopyToResponse) -> Result<Self, TryFromProtoError> {
        use proto_copy_to_response::Kind::*;
        match proto.kind {
            Some(Rows(rows)) => Ok(CopyToResponse::RowCount(rows)),
            Some(Error(error)) => Ok(CopyToResponse::Error(error)),
            Some(Dropped(())) => Ok(CopyToResponse::Dropped),
            None => Err(TryFromProtoError::missing_field(
                "ProtoCopyToResponse::kind",
            )),
        }
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
pub struct SubscribeBatch<T = mz_repr::Timestamp> {
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
                                diff: d.into_proto(),
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
                            update.diff.into(),
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

/// Status updates replicas can report to the controller.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum StatusResponse {
    /// No status responses are implemeneted currently, but we're leaving the infrastructure around
    /// in anticipation of materialize#31246.
    Placeholder,
}

impl RustType<ProtoStatusResponse> for StatusResponse {
    fn into_proto(&self) -> ProtoStatusResponse {
        use proto_status_response::Kind;

        let kind = match self {
            Self::Placeholder => Kind::Placeholder(()),
        };
        ProtoStatusResponse { kind: Some(kind) }
    }

    fn from_proto(proto: ProtoStatusResponse) -> Result<Self, TryFromProtoError> {
        use proto_status_response::Kind;

        match proto.kind {
            Some(Kind::Placeholder(())) => Ok(Self::Placeholder),
            None => Err(TryFromProtoError::missing_field(
                "ProtoStatusResponse::kind",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::assert_ok;
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::ProptestConfig;
    use proptest::proptest;

    use super::*;

    /// Test to ensure the size of the `ComputeResponse` enum doesn't regress.
    #[mz_ore::test]
    fn test_compute_response_size() {
        assert_eq!(std::mem::size_of::<ComputeResponse>(), 120);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[mz_ore::test]
        fn compute_response_protobuf_roundtrip(expect in any::<ComputeResponse<mz_repr::Timestamp>>() ) {
            let actual = protobuf_roundtrip::<_, ProtoComputeResponse>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }
}

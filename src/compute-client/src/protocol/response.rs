// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compute protocol responses.

use mz_expr::row::RowCollection;
use mz_ore::cast::CastFrom;
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_client::batch::ProtoBatch;
use mz_persist_types::ShardId;
use mz_repr::{Diff, GlobalId, RelationDesc, Row};
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use uuid::Uuid;

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
    /// Batches of Rows, must be combined with responses from other workers and
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

/// Status updates replicas can report to the controller.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum StatusResponse {
    /// No status responses are implemented currently, but we're leaving the infrastructure around
    /// in anticipation of materialize#31246.
    Placeholder,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test to ensure the size of the `ComputeResponse` enum doesn't regress.
    #[mz_ore::test]
    fn test_compute_response_size() {
        assert_eq!(std::mem::size_of::<ComputeResponse>(), 120);
    }
}

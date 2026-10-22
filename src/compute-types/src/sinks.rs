// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for describing dataflow sinks.

use mz_expr::{ColumnOrder, MfpPlan};
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{CatalogItemId, GlobalId, RelationDesc, Timestamp};
use mz_storage_types::connections::aws::AwsConnection;
use mz_storage_types::sinks::S3UploadInfo;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;
use uuid::Uuid;

use crate::plan::scalar::LirScalarExpr;

/// A sink for updates to a relational collection.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ComputeSinkDesc<S: 'static = ()> {
    /// TODO(database-issues#7533): Add documentation.
    pub from: GlobalId,
    /// TODO(database-issues#7533): Add documentation.
    pub from_desc: RelationDesc,
    /// TODO(database-issues#7533): Add documentation.
    pub connection: ComputeSinkConnection<S>,
    /// TODO(database-issues#7533): Add documentation.
    pub with_snapshot: bool,
    /// TODO(database-issues#7533): Add documentation.
    pub up_to: Antichain<Timestamp>,
    /// TODO(database-issues#7533): Add documentation.
    pub non_null_assertions: Vec<usize>,
    /// TODO(database-issues#7533): Add documentation.
    pub refresh_schedule: Option<RefreshSchedule>,
    /// The arrangement of `from` the renderer consumes, and how to read rows from it.
    ///
    /// `None` means consume the unarranged collection. `Some((key, mfp))` names an arrangement
    /// of `from` by its key, and `mfp` reconstructs full rows of `from_desc` from the
    /// arrangement's key and value.
    ///
    /// Chosen during LIR lowering, where the available arrangements of `from` are known. It is
    /// `None` before lowering runs.
    pub from_arrangement: Option<(Vec<LirScalarExpr>, MfpPlan<LirScalarExpr>)>,
}

/// TODO(database-issues#7533): Add documentation.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ComputeSinkConnection<S: 'static = ()> {
    /// TODO(database-issues#7533): Add documentation.
    Subscribe(SubscribeSinkConnection),
    /// A subscribe that reports its output as batches of descriptions.
    BatchedSubscribe(BatchedSubscribeSinkConnection),
    /// TODO(database-issues#7533): Add documentation.
    MaterializedView(MaterializedViewSinkConnection<S>),
    /// A compute sink to do a oneshot copy to s3.
    CopyToS3Oneshot(CopyToS3OneshotSinkConnection),
    /// A compute sink that writes rows into the in-process Prometheus metrics registry.
    MetricSink(MetricSinkConnection),
}

impl<S> ComputeSinkConnection<S> {
    /// Returns the name of the sink connection.
    pub fn name(&self) -> &'static str {
        match self {
            ComputeSinkConnection::Subscribe(_) => "subscribe",
            ComputeSinkConnection::BatchedSubscribe(_) => "batched_subscribe",
            ComputeSinkConnection::MaterializedView(_) => "materialized_view",
            ComputeSinkConnection::CopyToS3Oneshot(_) => "copy_to_s3_oneshot",
            ComputeSinkConnection::MetricSink(_) => "metric_sink",
        }
    }

    /// True if the sink is a subscribe, which is differently recoverable than other sinks.
    pub fn is_subscribe(&self) -> bool {
        if let ComputeSinkConnection::Subscribe(_) = self {
            true
        } else {
            false
        }
    }

    /// Adapts the connection to a rendering at `as_of`, which must not be before the dataflow's
    /// previous as-of. Call it only when the as-of was raised.
    pub fn advance_as_of(&mut self, as_of: &Antichain<Timestamp>) {
        match self {
            ComputeSinkConnection::BatchedSubscribe(connection) => connection.advance_as_of(as_of),
            ComputeSinkConnection::Subscribe(_)
            | ComputeSinkConnection::MaterializedView(_)
            | ComputeSinkConnection::CopyToS3Oneshot(_)
            | ComputeSinkConnection::MetricSink(_) => {}
        }
    }
}

/// TODO(database-issues#7533): Add documentation.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SubscribeSinkConnection {
    /// An ordering for the data in the subscribe.
    pub output: Vec<ColumnOrder>,
}

/// Connection attributes of a subscribe that reports its output as batches of descriptions.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BatchedSubscribeSinkConnection {
    /// Names the stash shard that the subscribe's stashed shares are written to.
    pub stash_id: Uuid,
    /// The earliest time at which this rendering reports updates and errors.
    pub start: Timestamp,
    /// The description of the subscribed relation.
    pub output_desc: RelationDesc,
}

impl BatchedSubscribeSinkConnection {
    /// Raises `start` to the time after `as_of`, for a rendering at a raised as-of. Never lowers
    /// `start`.
    pub fn advance_as_of(&mut self, as_of: &Antichain<Timestamp>) {
        // A raised as-of is at most `U - 1`, the controller's read hold below an accepted upper
        // `U`. With a snapshot, an error standing at the as-of already poisoned an accepted batch,
        // which retired the subscribe. Without one, errors before the original start are exactly
        // those the subscribe must not report. So nothing at the as-of is left to report, and
        // counting an error standing there would fail the subscribe spuriously.
        let next = as_of.as_option().and_then(|as_of| as_of.try_step_forward());
        if let Some(next) = next {
            self.start = std::cmp::max(self.start, next);
        }
    }
}

/// Connection for a sink that publishes rows into the in-process Prometheus metrics registry.
///
/// The sink does not write to persist, so there is no storage metadata to parameterize over.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MetricSinkConnection {
    /// Value of the `sink` label on the sink's health gauges. A user sink passes its `GlobalId`,
    /// which is durable. A coordinator-installed curated sink passes its definition name, because
    /// its `GlobalId` is transient and would churn the label on every boot.
    pub label: String,
}

/// Connection attributes required to do a oneshot copy to s3.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CopyToS3OneshotSinkConnection {
    /// Information specific to the upload.
    pub upload_info: S3UploadInfo,
    /// The AWS connection information to do the writes.
    pub aws_connection: AwsConnection,
    /// The ID of the Connection object, used to generate the External ID when
    /// using AssumeRole with AWS connection.
    pub connection_id: CatalogItemId,
    /// The number of batches the COPY TO output will be divided into
    /// where each worker will process 0 or more batches of data.
    pub output_batch_count: u64,
}

/// TODO(database-issues#7533): Add documentation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializedViewSinkConnection<S> {
    /// TODO(database-issues#7533): Add documentation.
    pub value_desc: RelationDesc,
    /// TODO(database-issues#7533): Add documentation.
    pub storage_metadata: S,
}

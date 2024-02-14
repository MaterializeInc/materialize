// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! These structure represents a full set up updates for the `mz_source_statistics_raw`
//! and `mz_sink_statistics_raw` tables for a specific source-worker/sink-worker pair.
//! They are structured like this for simplicity
//! and efficiency: Each storage worker can individually collect and consolidate metrics,
//! then control how much `StorageResponse` traffic is produced when sending updates
//! back to the controller to be written.
//!
//! The proto conversions for this types are in the `client` module, for now.

use serde::{Deserialize, Serialize};

use mz_ore::cast::CastFrom;
use mz_proto::{IntoRustIfSome, RustType, TryFromProtoError};
use mz_repr::{GlobalId, RelationDesc, ScalarType};
use once_cell::sync::Lazy;

include!(concat!(env!("OUT_DIR"), "/mz_storage_client.statistics.rs"));

pub static MZ_SOURCE_STATISTICS_RAW_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        // Id of the source (or subsource).
        .with_column("id", ScalarType::String.nullable(false))
        //
        // Counters
        //
        // A counter of the messages we have read from upstream for this source.
        // Never resets.
        .with_column("messages_received", ScalarType::UInt64.nullable(false))
        // A counter of the bytes we have read from upstream for this source.
        // Never resets.
        .with_column("bytes_received", ScalarType::UInt64.nullable(false))
        // A counter of the updates we have staged to commit for this source.
        // Never resets.
        .with_column("updates_staged", ScalarType::UInt64.nullable(false))
        // A counter of the updates we have committed for this source.
        // Never resets.
        .with_column("updates_committed", ScalarType::UInt64.nullable(false))
        //
        // Resetting gauges
        //
        // A gauge of the number of records in the envelope state. 0 for sources
        // Resetted when the source is restarted, for any reason.
        .with_column("envelope_state_records", ScalarType::UInt64.nullable(false))
        // A gauge of the number of bytes in the envelope state. 0 for sources
        // Resetted when the source is restarted, for any reason.
        .with_column("envelope_state_bytes", ScalarType::UInt64.nullable(false))
        // A gauge that shows the duration of rehydration. `NULL` before rehydration
        // is done.
        // Resetted when the source is restarted, for any reason.
        .with_column("rehydration_latency", ScalarType::Interval.nullable(true))
        // A gauge of the number of _values_ (source defined unit) the _snapshot_ of this source
        // contains.
        // Sometimes resetted when the source can snapshot new pieces of upstream (like Postgres and
        // MySql).
        // (like pg and mysql) may repopulate this column when tables are added.
        //
        // `NULL` while we discover the snapshot size.
        .with_column("snapshot_total", ScalarType::UInt64.nullable(true))
        // A gauge of the number of _values_ (source defined unit) we have read of the _snapshot_
        // of this source.
        // Sometimes resetted when the source can snapshot new pieces of upstream (like Postgres and
        // MySql).
        //
        // `NULL` while we discover the snapshot size.
        .with_column("snapshot_read", ScalarType::UInt64.nullable(true))
        //
        // Non-resetting gauges
        //
        // Whether or not the snapshot for the source has been committed. Never resets.
        .with_column("snapshot_committed", ScalarType::Bool.nullable(false))
        // The following are not yet reported by sources and have 0 or `NULL` values.
        // They have been added here to reduce churn changing the schema of this collection.
        //
        // A gauge of the number of _values_ (source defined unit) available to be read from upstream.
        // Never resets. Not to be confused with any of the counters above.
        .with_column("upstream_values", ScalarType::UInt64.nullable(false))
        // A gauge of the number of _values_ (source defined unit) we have committed.
        // Never resets. Not to be confused with any of the counters above.
        .with_column("committed_values", ScalarType::UInt64.nullable(false))
});

pub static MZ_SINK_STATISTICS_RAW_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        // Id of the sink.
        .with_column("id", ScalarType::String.nullable(false))
        //
        // Counters
        //
        // A counter of the messages we have staged to upstream.
        // Never resets.
        .with_column("messages_staged", ScalarType::UInt64.nullable(false))
        // A counter of the messages we have committed.
        // Never resets.
        .with_column("messages_committed", ScalarType::UInt64.nullable(false))
        // A counter of the bytes we have staged to upstream.
        // Never resets.
        .with_column("bytes_staged", ScalarType::UInt64.nullable(false))
        // A counter of the bytes we have committed.
        // Never resets.
        .with_column("bytes_committed", ScalarType::UInt64.nullable(false))
});

/// A trait that abstracts over user-facing statistics objects, used
/// by `spawn_statistics_scraper`.
pub trait PackableStats {
    /// Pack `self` into the `Row`.
    fn pack(&self, packer: mz_repr::RowPacker<'_>);
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SourceStatisticsUpdate {
    pub id: GlobalId,
    pub worker_id: usize,
    pub snapshot_committed: bool,
    pub messages_received: u64,
    pub bytes_received: u64,
    pub updates_staged: u64,
    pub updates_committed: u64,
    pub envelope_state_bytes: u64,
    pub envelope_state_records: u64,
    pub rehydration_latency_ms: Option<i64>,
}

impl PackableStats for SourceStatisticsUpdate {
    fn pack(&self, mut packer: mz_repr::RowPacker<'_>) {
        use mz_repr::Datum;
        // id
        packer.push(Datum::from(self.id.to_string().as_str()));
        packer.push(Datum::from(u64::cast_from(self.worker_id)));
        packer.push(Datum::from(self.snapshot_committed));
        packer.push(Datum::from(self.messages_received));
        packer.push(Datum::from(self.bytes_received));
        packer.push(Datum::from(self.updates_staged));
        packer.push(Datum::from(self.updates_committed));
        packer.push(Datum::from(self.envelope_state_bytes));
        packer.push(Datum::from(self.envelope_state_records));
        packer.push(Datum::from(
            self.rehydration_latency_ms
                .map(chrono::Duration::milliseconds),
        ));
    }
}

impl RustType<ProtoSourceStatisticsUpdate> for SourceStatisticsUpdate {
    fn into_proto(&self) -> ProtoSourceStatisticsUpdate {
        ProtoSourceStatisticsUpdate {
            id: Some(self.id.into_proto()),

            worker_id: u64::cast_from(self.worker_id),

            messages_received: self.messages_received,
            bytes_received: self.bytes_received,
            updates_staged: self.updates_staged,
            updates_committed: self.updates_committed,

            envelope_state_records: self.envelope_state_records,
            envelope_state_bytes: self.envelope_state_bytes,
            rehydration_latency_ms: self.rehydration_latency_ms,

            snapshot_committed: self.snapshot_committed,
        }
    }

    fn from_proto(proto: ProtoSourceStatisticsUpdate) -> Result<Self, TryFromProtoError> {
        Ok(SourceStatisticsUpdate {
            id: proto
                .id
                .into_rust_if_some("ProtoSourceStatisticsUpdate::id")?,

            worker_id: usize::cast_from(proto.worker_id),

            messages_received: proto.messages_received,
            bytes_received: proto.bytes_received,
            updates_staged: proto.updates_staged,
            updates_committed: proto.updates_committed,

            envelope_state_records: proto.envelope_state_records,
            envelope_state_bytes: proto.envelope_state_bytes,
            rehydration_latency_ms: proto.rehydration_latency_ms,

            snapshot_committed: proto.snapshot_committed,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SinkStatisticsUpdate {
    pub id: GlobalId,
    pub worker_id: usize,
    pub messages_staged: u64,
    pub messages_committed: u64,
    pub bytes_staged: u64,
    pub bytes_committed: u64,
}

impl PackableStats for SinkStatisticsUpdate {
    fn pack(&self, mut packer: mz_repr::RowPacker<'_>) {
        use mz_repr::Datum;
        packer.push(Datum::from(self.id.to_string().as_str()));
        packer.push(Datum::from(u64::cast_from(self.worker_id)));
        packer.push(Datum::from(self.messages_staged));
        packer.push(Datum::from(self.messages_committed));
        packer.push(Datum::from(self.bytes_staged));
        packer.push(Datum::from(self.bytes_committed));
    }
}

impl RustType<ProtoSinkStatisticsUpdate> for SinkStatisticsUpdate {
    fn into_proto(&self) -> ProtoSinkStatisticsUpdate {
        ProtoSinkStatisticsUpdate {
            id: Some(self.id.into_proto()),

            worker_id: u64::cast_from(self.worker_id),

            messages_staged: self.messages_staged,
            messages_committed: self.messages_committed,
            bytes_staged: self.bytes_staged,
            bytes_committed: self.bytes_committed,
        }
    }

    fn from_proto(proto: ProtoSinkStatisticsUpdate) -> Result<Self, TryFromProtoError> {
        Ok(SinkStatisticsUpdate {
            id: proto
                .id
                .into_rust_if_some("ProtoSinkStatisticsUpdate::id")?,

            worker_id: usize::cast_from(proto.worker_id),

            messages_staged: proto.messages_staged,
            messages_committed: proto.messages_committed,
            bytes_staged: proto.bytes_staged,
            bytes_committed: proto.bytes_committed,
        })
    }
}

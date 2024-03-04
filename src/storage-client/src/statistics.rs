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

use mz_proto::{IntoRustIfSome, RustType, TryFromProtoError};
use mz_repr::{GlobalId, RelationDesc, Row, ScalarType};
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
        .with_column("records_indexed", ScalarType::UInt64.nullable(false))
        // A gauge of the number of bytes in the envelope state. 0 for sources
        // Resetted when the source is restarted, for any reason.
        .with_column("bytes_indexed", ScalarType::UInt64.nullable(false))
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
        .with_column("snapshot_records_known", ScalarType::UInt64.nullable(true))
        // A gauge of the number of _values_ (source defined unit) we have read of the _snapshot_
        // of this source.
        // Sometimes resetted when the source can snapshot new pieces of upstream (like Postgres and
        // MySql).
        //
        // `NULL` while we discover the snapshot size.
        .with_column("snapshot_records_staged", ScalarType::UInt64.nullable(true))
        //
        // Non-resetting gauges
        //
        // Whether or not the snapshot for the source has been committed. Never resets.
        .with_column("snapshot_committed", ScalarType::Bool.nullable(false))
        // The following are not yet reported by sources and have 0 or `NULL` values.
        // They have been added here to reduce churn changing the schema of this collection.
        //
        // These are left nullable for now in case we want semantics for `NULL` values. We
        // currently never expose null values.
        //
        // A gauge of the number of _values_ (source defined unit) available to be read from upstream.
        // Never resets. Not to be confused with any of the counters above.
        .with_column("offset_known", ScalarType::UInt64.nullable(true))
        // A gauge of the number of _values_ (source defined unit) we have committed.
        // Never resets. Not to be confused with any of the counters above.
        .with_column("offset_committed", ScalarType::UInt64.nullable(true))
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

// Types of statistics (counter and various types of gauges), that have different semantics
// when sources/sinks are reset.

/// A trait that defines the semantics storage statistics are able to have
pub trait StorageMetric {
    /// Summarizes a set of measurements into a single representative value.
    /// Typically this function is used to summarize the measurements collected from each worker.
    fn summarize<'a, I>(values: I) -> Self
    where
        I: IntoIterator<Item = &'a Self>,
        Self: Sized + 'a;

    /// Incorporate this value with another.
    fn incorporate(&mut self, other: Self, field_name: &'static str);
}

/// A counter that never resets.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Counter(u64);

impl StorageMetric for Counter {
    fn summarize<'a, I>(values: I) -> Self
    where
        I: IntoIterator<Item = &'a Self>,
        Self: Sized + 'a,
    {
        // Sum across workers.
        Self(values.into_iter().map(|c| c.0).sum())
    }

    fn incorporate(&mut self, other: Self, _field_name: &'static str) {
        // Always add the new value to the existing one.
        self.0 += other.0
    }
}

impl From<u64> for Counter {
    fn from(f: u64) -> Self {
        Counter(f)
    }
}

/// A latency gauge that is reset on every restart.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ResettingLatency(Option<i64>);

impl From<Option<i64>> for ResettingLatency {
    fn from(f: Option<i64>) -> Self {
        ResettingLatency(f)
    }
}

impl StorageMetric for ResettingLatency {
    fn summarize<'a, I>(values: I) -> Self
    where
        I: IntoIterator<Item = &'a Self>,
        Self: Sized + 'a,
    {
        let mut max = 0;
        for value in values {
            match value.0 {
                // If any of the values are `NULL`, then we don't yet know the max.
                None => return Self(None),
                // Pick the worst latency across workers.
                Some(value) => max = std::cmp::max(max, value),
            }
        }

        Self(Some(max))
    }

    fn incorporate(&mut self, other: Self, _field_name: &'static str) {
        // Reset to the new value.
        self.0 = other.0;
    }
}

/// A numerical gauge that is always resets, but can start out as `NULL`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct ResettingNullableTotal(Option<u64>);

impl StorageMetric for ResettingNullableTotal {
    fn summarize<'a, I>(values: I) -> Self
    where
        I: IntoIterator<Item = &'a Self>,
        Self: Sized + 'a,
    {
        let mut sum = 0;
        for value in values {
            match value.0 {
                // If any of the values are `NULL`, then we merge to `NULL`
                None => return Self(None),
                // Pick the worst latency across workers.
                Some(value) => sum += value,
            }
        }

        Self(Some(sum))
    }

    fn incorporate(&mut self, other: Self, _field_name: &'static str) {
        match (&mut self.0, other.0) {
            (None, other) => {
                self.0 = other;
            }
            // Override the total.
            (Some(this), Some(other)) => *this = other,
            (Some(_), None) => {
                // `NULL`'s don't reset the value.
            }
        }
    }
}

impl From<Option<u64>> for ResettingNullableTotal {
    fn from(f: Option<u64>) -> Self {
        ResettingNullableTotal(f)
    }
}
/// A numerical gauge that is always resets.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct ResettingTotal(u64);

impl StorageMetric for ResettingTotal {
    fn summarize<'a, I>(values: I) -> Self
    where
        I: IntoIterator<Item = &'a Self>,
        Self: Sized + 'a,
    {
        // Sum across workers.
        Self(values.into_iter().map(|c| c.0).sum())
    }

    fn incorporate(&mut self, other: Self, _field_name: &'static str) {
        // Reset the pre-existing value.
        self.0 = other.0;
    }
}

impl From<u64> for ResettingTotal {
    fn from(f: u64) -> Self {
        ResettingTotal(f)
    }
}

/// A boolean gauge that is never reset.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Boolean(bool);

impl StorageMetric for Boolean {
    fn summarize<'a, I>(values: I) -> Self
    where
        I: IntoIterator<Item = &'a Self>,
        Self: Sized + 'a,
    {
        // All workers must be true for this gauge to be true.
        Self(values.into_iter().fold(true, |base, new| base & new.0))
    }

    fn incorporate(&mut self, other: Self, field_name: &'static str) {
        // A boolean regressing to `false` is a bug.
        //
        // Clippy's suggestion here is not good!
        #[allow(clippy::bool_comparison)]
        if other.0 < self.0 {
            tracing::error!(
                "boolean gauge for field {field_name} erroneously regressed from true to false",
            );
            return;
        }
        self.0 = other.0;
    }
}

impl From<bool> for Boolean {
    fn from(f: bool) -> Self {
        Boolean(f)
    }
}

/// A numerical gauge that is never resets.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct Total(u64);

impl From<u64> for Total {
    fn from(f: u64) -> Self {
        Total(f)
    }
}

impl StorageMetric for Total {
    fn summarize<'a, I>(values: I) -> Self
    where
        I: IntoIterator<Item = &'a Self>,
        Self: Sized + 'a,
    {
        // Sum across workers.
        Self(values.into_iter().map(|c| c.0).sum())
    }

    fn incorporate(&mut self, other: Self, field_name: &'static str) {
        // A `Total` regressing to is a bug.
        if other.0 < self.0 {
            tracing::error!(
                "total gauge {field_name} erroneously regressed from {} to {}",
                self.0,
                other.0
            );
            return;
        }
        self.0 = other.0;
    }
}

/// A gauge that has semantics based on the `StorageMetric` implementation of its inner.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Gauge<T>(T);

impl<T> Gauge<T> {
    // This can't be a `From` impl cause of coherence issues :(
    pub fn gauge<F>(f: F) -> Self
    where
        T: From<F>,
    {
        Gauge(f.into())
    }
}

impl<T: StorageMetric> StorageMetric for Gauge<T> {
    fn summarize<'a, I>(values: I) -> Self
    where
        I: IntoIterator<Item = &'a Self>,
        Self: Sized + 'a,
    {
        Gauge(T::summarize(values.into_iter().map(|i| &i.0)))
    }

    fn incorporate(&mut self, other: Self, field_name: &'static str) {
        self.0.incorporate(other.0, field_name)
    }
}

/// A gauge that has semantics based on the `StorageMetric` implementation of its inner.
/// Skippable gauges do not need to be given a value when updated, defaulting to maintaining
/// the default value.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SkippableGauge<T>(Option<T>);

impl<T> SkippableGauge<T> {
    // This can't be a `From` impl cause of coherence issues :(
    pub fn gauge<F>(f: Option<F>) -> Self
    where
        T: From<F>,
    {
        SkippableGauge(f.map(Into::into))
    }
}

impl<T: StorageMetric + Default + Clone + std::fmt::Debug> SkippableGauge<T> {
    fn summarize<'a, I>(values: I) -> Self
    where
        I: IntoIterator<Item = &'a Self>,
        Self: Sized + 'a,
    {
        let mut any_none = false;

        let inner = T::summarize(values.into_iter().filter_map(|i| {
            any_none |= i.0.is_none();
            i.0.as_ref()
        }));

        // If any are none, we can't aggregate.
        Self((!any_none).then_some(inner))
    }

    fn incorporate(&mut self, other: Self, field_name: &'static str) {
        match (&mut self.0, other.0) {
            (_, None) => {}
            (None, Some(other)) => self.0 = Some(other),
            (Some(this), Some(other)) => this.incorporate(other, field_name),
        }
    }
}

impl<T: Default + Clone> SkippableGauge<T> {
    fn pack(&self) -> T {
        self.0.clone().unwrap_or_default()
    }
}

/// A trait that abstracts over user-facing statistics objects, used
/// by `spawn_statistics_scraper`.
pub trait PackableStats {
    /// Pack `self` into the `Row`.
    fn pack(&self, packer: mz_repr::RowPacker<'_>);
    /// Unpack a `Row` back into a `Self`.
    fn unpack(row: Row) -> (GlobalId, Self);
}

/// An update as reported from a storage instance. The semantics
/// of each field are documented above in `MZ_SOURCE_STATISTICS_RAW_DESC`,
/// and encoded in the field types.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SourceStatisticsUpdate {
    pub id: GlobalId,

    pub messages_received: Counter,
    pub bytes_received: Counter,
    pub updates_staged: Counter,
    pub updates_committed: Counter,

    pub records_indexed: Gauge<ResettingTotal>,
    pub bytes_indexed: Gauge<ResettingTotal>,
    pub rehydration_latency_ms: Gauge<ResettingLatency>,
    pub snapshot_records_known: Gauge<ResettingNullableTotal>,
    pub snapshot_records_staged: Gauge<ResettingNullableTotal>,

    pub snapshot_committed: Gauge<Boolean>,
    pub offset_known: SkippableGauge<Total>,
    pub offset_committed: SkippableGauge<Total>,
}

impl SourceStatisticsUpdate {
    pub fn summarize<'a, I, F>(values: F) -> Self
    where
        I: IntoIterator<Item = &'a Self>,
        F: Fn() -> I,
        Self: 'a,
    {
        SourceStatisticsUpdate {
            id: values().into_iter().next().unwrap().id,
            messages_received: Counter::summarize(
                values().into_iter().map(|s| &s.messages_received),
            ),
            bytes_received: Counter::summarize(values().into_iter().map(|s| &s.bytes_received)),
            updates_staged: Counter::summarize(values().into_iter().map(|s| &s.updates_staged)),
            updates_committed: Counter::summarize(
                values().into_iter().map(|s| &s.updates_committed),
            ),
            records_indexed: Gauge::summarize(values().into_iter().map(|s| &s.records_indexed)),
            bytes_indexed: Gauge::summarize(values().into_iter().map(|s| &s.bytes_indexed)),
            rehydration_latency_ms: Gauge::summarize(
                values().into_iter().map(|s| &s.rehydration_latency_ms),
            ),
            snapshot_records_known: Gauge::summarize(
                values().into_iter().map(|s| &s.snapshot_records_known),
            ),
            snapshot_records_staged: Gauge::summarize(
                values().into_iter().map(|s| &s.snapshot_records_staged),
            ),
            snapshot_committed: Gauge::summarize(
                values().into_iter().map(|s| &s.snapshot_committed),
            ),
            offset_known: SkippableGauge::summarize(values().into_iter().map(|s| &s.offset_known)),
            offset_committed: SkippableGauge::summarize(
                values().into_iter().map(|s| &s.offset_committed),
            ),
        }
    }

    /// Reset counters so that we continue to ship diffs to the controller.
    pub fn reset_counters(&mut self) {
        self.messages_received.0 = 0;
        self.bytes_received.0 = 0;
        self.updates_staged.0 = 0;
        self.updates_committed.0 = 0;
    }

    pub fn incorporate(&mut self, other: SourceStatisticsUpdate) {
        let SourceStatisticsUpdate {
            messages_received,
            bytes_received,
            updates_staged,
            updates_committed,
            records_indexed,
            bytes_indexed,
            rehydration_latency_ms,
            snapshot_records_known,
            snapshot_records_staged,
            snapshot_committed,
            offset_known,
            offset_committed,
            ..
        } = self;

        messages_received.incorporate(other.messages_received, "messages_received");
        bytes_received.incorporate(other.bytes_received, "bytes_received");
        updates_staged.incorporate(other.updates_staged, "updates_staged");
        updates_committed.incorporate(other.updates_committed, "updates_committed");
        records_indexed.incorporate(other.records_indexed, "records_indexed");
        bytes_indexed.incorporate(other.bytes_indexed, "bytes_indexed");
        rehydration_latency_ms.incorporate(other.rehydration_latency_ms, "rehydration_latency_ms");
        snapshot_records_known.incorporate(other.snapshot_records_known, "snapshot_records_known");
        snapshot_records_staged
            .incorporate(other.snapshot_records_staged, "snapshot_records_staged");
        snapshot_committed.incorporate(other.snapshot_committed, "snapshot_committed");
        offset_known.incorporate(other.offset_known, "offset_known");
        offset_committed.incorporate(other.offset_committed, "offset_committed");
    }
}

impl PackableStats for SourceStatisticsUpdate {
    fn pack(&self, mut packer: mz_repr::RowPacker<'_>) {
        use mz_repr::Datum;
        // id
        packer.push(Datum::from(self.id.to_string().as_str()));
        // Counters.
        packer.push(Datum::from(self.messages_received.0));
        packer.push(Datum::from(self.bytes_received.0));
        packer.push(Datum::from(self.updates_staged.0));
        packer.push(Datum::from(self.updates_committed.0));
        // Resetting gauges.
        packer.push(Datum::from(self.records_indexed.0 .0));
        packer.push(Datum::from(self.bytes_indexed.0 .0));
        let rehydration_latency = self
            .rehydration_latency_ms
            .0
             .0
            .map(|ms| mz_repr::adt::interval::Interval::new(0, 0, ms * 1000));
        packer.push(Datum::from(rehydration_latency));
        packer.push(Datum::from(self.snapshot_records_known.0 .0));
        packer.push(Datum::from(self.snapshot_records_staged.0 .0));
        // Gauges
        packer.push(Datum::from(self.snapshot_committed.0 .0));
        packer.push(Datum::from(self.offset_known.pack().0));
        packer.push(Datum::from(self.offset_committed.pack().0));
    }

    fn unpack(row: Row) -> (GlobalId, Self) {
        let mut iter = row.iter();
        let s = Self {
            id: iter.next().unwrap().unwrap_str().parse().unwrap(),

            messages_received: iter.next().unwrap().unwrap_uint64().into(),
            bytes_received: iter.next().unwrap().unwrap_uint64().into(),
            updates_staged: iter.next().unwrap().unwrap_uint64().into(),
            updates_committed: iter.next().unwrap().unwrap_uint64().into(),

            records_indexed: Gauge::gauge(iter.next().unwrap().unwrap_uint64()),
            bytes_indexed: Gauge::gauge(iter.next().unwrap().unwrap_uint64()),
            rehydration_latency_ms: Gauge::gauge(
                <Option<mz_repr::adt::interval::Interval>>::try_from(iter.next().unwrap())
                    .unwrap()
                    .map(|int| int.micros / 1000),
            ),
            snapshot_records_known: Gauge::gauge(
                <Option<u64>>::try_from(iter.next().unwrap()).unwrap(),
            ),
            snapshot_records_staged: Gauge::gauge(
                <Option<u64>>::try_from(iter.next().unwrap()).unwrap(),
            ),

            snapshot_committed: Gauge::gauge(iter.next().unwrap().unwrap_bool()),
            offset_known: SkippableGauge::gauge(Some(iter.next().unwrap().unwrap_uint64())),
            offset_committed: SkippableGauge::gauge(Some(iter.next().unwrap().unwrap_uint64())),
        };

        (s.id, s)
    }
}

impl RustType<ProtoSourceStatisticsUpdate> for SourceStatisticsUpdate {
    fn into_proto(&self) -> ProtoSourceStatisticsUpdate {
        ProtoSourceStatisticsUpdate {
            id: Some(self.id.into_proto()),

            messages_received: self.messages_received.0,
            bytes_received: self.bytes_received.0,
            updates_staged: self.updates_staged.0,
            updates_committed: self.updates_committed.0,

            records_indexed: self.records_indexed.0 .0,
            bytes_indexed: self.bytes_indexed.0 .0,
            rehydration_latency_ms: self.rehydration_latency_ms.0 .0,
            snapshot_records_known: self.snapshot_records_known.0 .0,
            snapshot_records_staged: self.snapshot_records_staged.0 .0,

            snapshot_committed: self.snapshot_committed.0 .0,
            offset_known: self.offset_known.0.clone().map(|i| i.0),
            offset_committed: self.offset_committed.0.clone().map(|i| i.0),
        }
    }

    fn from_proto(proto: ProtoSourceStatisticsUpdate) -> Result<Self, TryFromProtoError> {
        Ok(SourceStatisticsUpdate {
            id: proto
                .id
                .into_rust_if_some("ProtoSourceStatisticsUpdate::id")?,

            messages_received: Counter(proto.messages_received),
            bytes_received: Counter(proto.bytes_received),
            updates_staged: Counter(proto.updates_staged),
            updates_committed: Counter(proto.updates_committed),

            records_indexed: Gauge::gauge(proto.records_indexed),
            bytes_indexed: Gauge::gauge(proto.bytes_indexed),
            rehydration_latency_ms: Gauge::gauge(proto.rehydration_latency_ms),
            snapshot_records_known: Gauge::gauge(proto.snapshot_records_known),
            snapshot_records_staged: Gauge::gauge(proto.snapshot_records_staged),

            snapshot_committed: Gauge::gauge(proto.snapshot_committed),
            offset_known: SkippableGauge::gauge(proto.offset_known),
            offset_committed: SkippableGauge::gauge(proto.offset_committed),
        })
    }
}

/// An update as reported from a storage instance. The semantics
/// of each field are documented above in `MZ_SINK_STATISTICS_RAW_DESC`,
/// and encoded in the field types.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SinkStatisticsUpdate {
    pub id: GlobalId,

    pub messages_staged: Counter,
    pub messages_committed: Counter,
    pub bytes_staged: Counter,
    pub bytes_committed: Counter,
}

impl SinkStatisticsUpdate {
    pub fn incorporate(&mut self, other: SinkStatisticsUpdate) {
        let SinkStatisticsUpdate {
            messages_staged,
            messages_committed,
            bytes_staged,
            bytes_committed,
            ..
        } = self;

        messages_staged.incorporate(other.messages_staged, "messages_staged");
        messages_committed.incorporate(other.messages_committed, "messages_committed");
        bytes_staged.incorporate(other.bytes_staged, "bytes_staged");
        bytes_committed.incorporate(other.bytes_committed, "bytes_committed");
    }

    pub fn summarize<'a, I, F>(values: F) -> Self
    where
        I: IntoIterator<Item = &'a Self>,
        F: Fn() -> I,
        Self: 'a,
    {
        SinkStatisticsUpdate {
            id: values().into_iter().next().unwrap().id,
            messages_staged: Counter::summarize(values().into_iter().map(|s| &s.messages_staged)),
            messages_committed: Counter::summarize(
                values().into_iter().map(|s| &s.messages_committed),
            ),
            bytes_staged: Counter::summarize(values().into_iter().map(|s| &s.bytes_staged)),
            bytes_committed: Counter::summarize(values().into_iter().map(|s| &s.bytes_committed)),
        }
    }

    /// Reset counters so that we continue to ship diffs to the controller.
    pub fn reset_counters(&mut self) {
        self.messages_staged.0 = 0;
        self.messages_committed.0 = 0;
        self.bytes_staged.0 = 0;
        self.bytes_committed.0 = 0;
    }
}

impl PackableStats for SinkStatisticsUpdate {
    fn pack(&self, mut packer: mz_repr::RowPacker<'_>) {
        use mz_repr::Datum;
        packer.push(Datum::from(self.id.to_string().as_str()));
        packer.push(Datum::from(self.messages_staged.0));
        packer.push(Datum::from(self.messages_committed.0));
        packer.push(Datum::from(self.bytes_staged.0));
        packer.push(Datum::from(self.bytes_committed.0));
    }

    fn unpack(row: Row) -> (GlobalId, Self) {
        let mut iter = row.iter();
        let s = Self {
            // Id
            id: iter.next().unwrap().unwrap_str().parse().unwrap(),
            // Counters
            messages_staged: iter.next().unwrap().unwrap_uint64().into(),
            messages_committed: iter.next().unwrap().unwrap_uint64().into(),
            bytes_staged: iter.next().unwrap().unwrap_uint64().into(),
            bytes_committed: iter.next().unwrap().unwrap_uint64().into(),
        };

        (s.id, s)
    }
}

impl RustType<ProtoSinkStatisticsUpdate> for SinkStatisticsUpdate {
    fn into_proto(&self) -> ProtoSinkStatisticsUpdate {
        ProtoSinkStatisticsUpdate {
            id: Some(self.id.into_proto()),

            messages_staged: self.messages_staged.0,
            messages_committed: self.messages_committed.0,
            bytes_staged: self.bytes_staged.0,
            bytes_committed: self.bytes_committed.0,
        }
    }

    fn from_proto(proto: ProtoSinkStatisticsUpdate) -> Result<Self, TryFromProtoError> {
        Ok(SinkStatisticsUpdate {
            id: proto
                .id
                .into_rust_if_some("ProtoSinkStatisticsUpdate::id")?,

            messages_staged: Counter(proto.messages_staged),
            messages_committed: Counter(proto.messages_committed),
            bytes_staged: Counter(proto.bytes_staged),
            bytes_committed: Counter(proto.bytes_committed),
        })
    }
}

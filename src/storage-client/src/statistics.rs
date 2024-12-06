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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use mz_proto::{IntoRustIfSome, RustType, TryFromProtoError};
use mz_repr::{GlobalId, RelationDesc, Row, ScalarType};

include!(concat!(env!("OUT_DIR"), "/mz_storage_client.statistics.rs"));

pub static MZ_SOURCE_STATISTICS_RAW_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
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
        .finish()
});

pub static MZ_SINK_STATISTICS_RAW_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
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
        .finish()
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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
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

impl ResettingLatency {
    fn reset(&mut self) {
        self.0 = None;
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

impl ResettingNullableTotal {
    fn reset(&mut self) {
        self.0 = None;
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

impl ResettingTotal {
    fn reset(&mut self) {
        self.0 = 0;
    }
}

/// A boolean gauge that is never reset.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
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

/// A numerical gauge that never regresses.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Total {
    /// Defaults to 0. Can be skipped on updates from clusterd.
    total: Option<u64>,
    /// If provided, it is bumped on regressions, as opposed to `error!`
    /// logs.
    #[serde(skip)]
    regressions: Option<
        mz_ore::metrics::DeleteOnDropCounter<'static, prometheus::core::AtomicU64, Vec<String>>,
    >,
}

impl From<Option<u64>> for Total {
    fn from(f: Option<u64>) -> Self {
        Total {
            total: f,
            regressions: None,
        }
    }
}

impl Clone for Total {
    fn clone(&self) -> Self {
        Self {
            total: self.total,
            regressions: None,
        }
    }
}

impl PartialEq for Total {
    fn eq(&self, other: &Self) -> bool {
        self.total == other.total
    }
}

impl Total {
    /// Pack this `Total` into a `u64`, defaulting to 0.
    fn pack(&self) -> u64 {
        self.total.unwrap_or_default()
    }
}

impl StorageMetric for Total {
    fn summarize<'a, I>(values: I) -> Self
    where
        I: IntoIterator<Item = &'a Self>,
        Self: Sized + 'a,
    {
        // Sum across workers, if all workers have participated
        // a non-`None` value.
        let mut any_none = false;

        let inner = values
            .into_iter()
            .filter_map(|i| {
                any_none |= i.total.is_none();
                i.total.as_ref()
            })
            .sum();

        // If any are none, we can't aggregate.
        // self.regressions is only meaningful in incorporation.
        Self {
            total: (!any_none).then_some(inner),
            regressions: None,
        }
    }

    fn incorporate(&mut self, other: Self, field_name: &'static str) {
        match (&mut self.total, other.total) {
            (_, None) => {}
            (None, Some(other)) => self.total = Some(other),
            (Some(this), Some(other)) => {
                if other < *this {
                    if let Some(metric) = &self.regressions {
                        metric.inc()
                    } else {
                        tracing::error!(
                            "total gauge {field_name} erroneously regressed from {} to {}",
                            this,
                            other
                        );
                    }
                    return;
                }
                *this = other
            }
        }
    }
}

/// A gauge that has semantics based on the `StorageMetric` implementation of its inner.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
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

/// A trait that abstracts over user-facing statistics objects, used
/// by `spawn_statistics_scraper`.
pub trait PackableStats {
    /// Pack `self` into the `Row`.
    fn pack(&self, packer: mz_repr::RowPacker<'_>);
    /// Unpack a `Row` back into a `Self`.
    fn unpack(row: Row, metrics: &crate::metrics::StorageControllerMetrics) -> (GlobalId, Self);
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
    // `offset_known` is enriched with a counter in `unpack` and `with_metrics` that is
    // bumped whenever it regresses. This is distinct from `offset_committed`, which
    // `error!` logs.
    //
    // `offset_committed` is entirely in our control: it is calculated from source frontiers
    // that are guaranteed to never go backwards. Therefore, it regresses is a bug in how we
    // calculate it.
    //
    // `offset_known` is calculated based on information the upstream service of the source gives
    // us. This is meaningfully less reliable, and can cause regressions in the value. Some known
    // cases that cause this are:
    // - A Kafka topic being deleted and recreated.
    // - A Postgres source being restored to a backup.
    //
    // We attempt to communicate both of these to the user using the source status system tables.
    // While emitting a regressed `offset_known` can be at least partially avoided in the source
    // implementation, we avoid noisy sentry alerts by instead bumping a counter that can be used
    // if a scenario requires more investigation.
    pub offset_known: Gauge<Total>,
    pub offset_committed: Gauge<Total>,
}

impl SourceStatisticsUpdate {
    pub fn new(id: GlobalId) -> Self {
        Self {
            id,
            messages_received: Default::default(),
            bytes_received: Default::default(),
            updates_staged: Default::default(),
            updates_committed: Default::default(),
            records_indexed: Default::default(),
            bytes_indexed: Default::default(),
            rehydration_latency_ms: Default::default(),
            snapshot_records_known: Default::default(),
            snapshot_records_staged: Default::default(),
            snapshot_committed: Default::default(),
            offset_known: Default::default(),
            offset_committed: Default::default(),
        }
    }

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
            offset_known: Gauge::summarize(values().into_iter().map(|s| &s.offset_known)),
            offset_committed: Gauge::summarize(values().into_iter().map(|s| &s.offset_committed)),
        }
    }

    /// Reset counters so that we continue to ship diffs to the controller.
    pub fn reset_counters(&mut self) {
        self.messages_received.0 = 0;
        self.bytes_received.0 = 0;
        self.updates_staged.0 = 0;
        self.updates_committed.0 = 0;
    }

    /// Reset all _resetable_ gauges to their default values.
    pub fn reset_gauges(&mut self) {
        self.records_indexed.0.reset();
        self.bytes_indexed.0.reset();
        self.rehydration_latency_ms.0.reset();
        self.snapshot_records_known.0.reset();
        self.snapshot_records_staged.0.reset();
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

    /// Incorporate only the counters of the given update, ignoring gauge values.
    pub fn incorporate_counters(&mut self, other: SourceStatisticsUpdate) {
        let SourceStatisticsUpdate {
            messages_received,
            bytes_received,
            updates_staged,
            updates_committed,
            ..
        } = self;

        messages_received.incorporate(other.messages_received, "messages_received");
        bytes_received.incorporate(other.bytes_received, "bytes_received");
        updates_staged.incorporate(other.updates_staged, "updates_staged");
        updates_committed.incorporate(other.updates_committed, "updates_committed");
    }

    /// Enrich statistics that use prometheus metrics.
    pub fn with_metrics(mut self, metrics: &crate::metrics::StorageControllerMetrics) -> Self {
        self.offset_known.0.regressions = Some(metrics.regressed_offset_known(self.id));
        self
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
        packer.push(Datum::from(self.offset_known.0.pack()));
        packer.push(Datum::from(self.offset_committed.0.pack()));
    }

    fn unpack(row: Row, metrics: &crate::metrics::StorageControllerMetrics) -> (GlobalId, Self) {
        let mut iter = row.iter();
        let mut s = Self {
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
            offset_known: Gauge::gauge(Some(iter.next().unwrap().unwrap_uint64())),
            offset_committed: Gauge::gauge(Some(iter.next().unwrap().unwrap_uint64())),
        };

        s.offset_known.0.regressions = Some(metrics.regressed_offset_known(s.id));
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
            offset_known: self.offset_known.0.total,
            offset_committed: self.offset_committed.0.total,
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
            offset_known: Gauge::gauge(proto.offset_known),
            offset_committed: Gauge::gauge(proto.offset_committed),
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
    pub fn new(id: GlobalId) -> Self {
        Self {
            id,
            messages_staged: Default::default(),
            messages_committed: Default::default(),
            bytes_staged: Default::default(),
            bytes_committed: Default::default(),
        }
    }

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

    /// Incorporate only the counters of the given update, ignoring gauge values.
    pub fn incorporate_counters(&mut self, other: SinkStatisticsUpdate) {
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

    /// Reset all _resetable_ gauges to their default values.
    pub fn reset_gauges(&self) {}
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

    fn unpack(row: Row, _metrics: &crate::metrics::StorageControllerMetrics) -> (GlobalId, Self) {
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

/// Statistics for webhooks.
#[derive(Default, Debug)]
pub struct WebhookStatistics {
    pub messages_received: AtomicU64,
    pub bytes_received: AtomicU64,
    pub updates_staged: AtomicU64,
    pub updates_committed: AtomicU64,
}

impl WebhookStatistics {
    /// Drain the current statistics into a `SourceStatisticsUpdate` with
    /// other values defaulted, resetting the atomic counters.
    pub fn drain_into_update(&self, id: GlobalId) -> SourceStatisticsUpdate {
        SourceStatisticsUpdate {
            id,
            messages_received: self.messages_received.swap(0, Ordering::Relaxed).into(),
            bytes_received: self.bytes_received.swap(0, Ordering::Relaxed).into(),
            updates_staged: self.updates_staged.swap(0, Ordering::Relaxed).into(),
            updates_committed: self.updates_committed.swap(0, Ordering::Relaxed).into(),
            records_indexed: Gauge::gauge(0),
            bytes_indexed: Gauge::gauge(0),
            rehydration_latency_ms: Gauge::gauge(None),
            snapshot_records_known: Gauge::gauge(None),
            snapshot_records_staged: Gauge::gauge(None),
            snapshot_committed: Gauge::gauge(true),
            offset_known: Gauge::gauge(None::<u64>),
            offset_committed: Gauge::gauge(None::<u64>),
        }
    }
}

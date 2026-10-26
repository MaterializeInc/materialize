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

use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use mz_controller_types::ReplicaId;
use mz_ore::metrics::DeleteOnDropCounter;
use serde::{Deserialize, Serialize};

use mz_repr::{GlobalId, RelationDesc, Row, SqlScalarType};

type RegressionCounter = DeleteOnDropCounter<prometheus::core::AtomicU64, Vec<String>>;

pub static MZ_SOURCE_STATISTICS_RAW_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        // Id of the source (or subsource).
        .with_column("id", SqlScalarType::String.nullable(false))
        // Id of the replica producing these statistics.
        .with_column("replica_id", SqlScalarType::String.nullable(true))
        //
        // Counters
        //
        // A counter of the messages we have read from upstream for this source.
        // Never resets.
        .with_column("messages_received", SqlScalarType::UInt64.nullable(false))
        // A counter of the bytes we have read from upstream for this source.
        // Never resets.
        .with_column("bytes_received", SqlScalarType::UInt64.nullable(false))
        // A counter of the updates we have staged to commit for this source.
        // Never resets.
        .with_column("updates_staged", SqlScalarType::UInt64.nullable(false))
        // A counter of the updates we have committed for this source.
        // Never resets.
        .with_column("updates_committed", SqlScalarType::UInt64.nullable(false))
        //
        // Resetting gauges
        //
        // A gauge of the number of records in the envelope state. 0 for sources
        // Resetted when the source is restarted, for any reason.
        .with_column("records_indexed", SqlScalarType::UInt64.nullable(false))
        // A gauge of the number of bytes in the envelope state. 0 for sources
        // Resetted when the source is restarted, for any reason.
        .with_column("bytes_indexed", SqlScalarType::UInt64.nullable(false))
        // A gauge that shows the duration of rehydration. `NULL` before rehydration
        // is done.
        // Resetted when the source is restarted, for any reason.
        .with_column(
            "rehydration_latency",
            SqlScalarType::Interval.nullable(true),
        )
        // A gauge of the number of _values_ (source defined unit) the _snapshot_ of this source
        // contains.
        // Sometimes resetted when the source can snapshot new pieces of upstream (like Postgres and
        // MySql).
        // (like pg and mysql) may repopulate this column when tables are added.
        //
        // `NULL` while we discover the snapshot size.
        .with_column(
            "snapshot_records_known",
            SqlScalarType::UInt64.nullable(true),
        )
        // A gauge of the number of _values_ (source defined unit) we have read of the _snapshot_
        // of this source.
        // Sometimes resetted when the source can snapshot new pieces of upstream (like Postgres and
        // MySql).
        //
        // `NULL` while we discover the snapshot size.
        .with_column(
            "snapshot_records_staged",
            SqlScalarType::UInt64.nullable(true),
        )
        //
        // Non-resetting gauges
        //
        // Whether or not the snapshot for the source has been committed. Never resets.
        .with_column("snapshot_committed", SqlScalarType::Bool.nullable(false))
        // The following are not yet reported by sources and have 0 or `NULL` values.
        // They have been added here to reduce churn changing the schema of this collection.
        //
        // These are left nullable for now in case we want semantics for `NULL` values. We
        // currently never expose null values.
        //
        // A gauge of the number of _values_ (source defined unit) available to be read from upstream.
        // Never resets. Not to be confused with any of the counters above.
        .with_column("offset_known", SqlScalarType::UInt64.nullable(true))
        // A gauge of the number of _values_ (source defined unit) we have committed.
        // Never resets. Not to be confused with any of the counters above.
        .with_column("offset_committed", SqlScalarType::UInt64.nullable(true))
        .finish()
});

pub static MZ_SINK_STATISTICS_RAW_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        // Id of the sink.
        .with_column("id", SqlScalarType::String.nullable(false))
        // Id of the replica producing these statistics.
        .with_column("replica_id", SqlScalarType::String.nullable(true))
        //
        // Counters
        //
        // A counter of the messages we have staged to upstream.
        // Never resets.
        .with_column("messages_staged", SqlScalarType::UInt64.nullable(false))
        // A counter of the messages we have committed.
        // Never resets.
        .with_column("messages_committed", SqlScalarType::UInt64.nullable(false))
        // A counter of the bytes we have staged to upstream.
        // Never resets.
        .with_column("bytes_staged", SqlScalarType::UInt64.nullable(false))
        // A counter of the bytes we have committed.
        // Never resets.
        .with_column("bytes_committed", SqlScalarType::UInt64.nullable(false))
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct Total {
    /// Defaults to 0. Can be skipped on updates from clusterd.
    total: Option<u64>,
}

impl From<Option<u64>> for Total {
    fn from(f: Option<u64>) -> Self {
        Total { total: f }
    }
}

impl Total {
    /// Pack this `Total` into a `u64`, defaulting to 0.
    fn pack(&self) -> u64 {
        self.total.unwrap_or_default()
    }

    /// Keeps the higher of the two values. Returns the refused `(current, incoming)` pair when
    /// `other` is lower, leaving what to do about it to the caller.
    fn merge(&mut self, other: Self) -> Option<(u64, u64)> {
        match (&mut self.total, other.total) {
            (_, None) => None,
            (None, Some(other)) => {
                self.total = Some(other);
                None
            }
            (Some(this), Some(other)) => {
                if other < *this {
                    return Some((*this, other));
                }
                *this = other;
                None
            }
        }
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
        Self {
            total: (!any_none).then_some(inner),
        }
    }

    fn incorporate(&mut self, other: Self, field_name: &'static str) {
        if let Some((from, to)) = self.merge(other) {
            tracing::error!("total gauge {field_name} erroneously regressed from {from} to {to}");
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

/// A trait that abstracts over user-facing statistics objects that can be
/// packed into a `Row` and unpacked again. That is they round-trip.
pub trait PackableStats {
    /// Pack `self` into the `Row`.
    fn pack(&self, packer: mz_repr::RowPacker<'_>);
    /// Unpack a `Row` back into a `Self`.
    fn unpack(
        row: Row,
        metrics: &crate::metrics::StorageControllerMetrics,
    ) -> (GlobalId, Option<ReplicaId>, Self);
}

/// Stats that can get out-of-date and should then get reaped.
pub trait ExpirableStats {
    /// The last time this bag of stats was updated (via an update coming in
    /// from a replica, presumably).
    fn last_updated(&self) -> Instant;
}

/// Stats that should be initialized with a row of zeroes, that is we want the
/// first data to show up in the builtin stats collection to be zeroes.
///
/// We do the tracking within the stat object itself instead of adding external
/// tracking. That's a bit of a roundabout way of doing it, but it works.
///
/// This is an odd requirement, and you would think we should just report what
/// is there when we get it, but this was consciously changed a while back and
/// the current Console incarnation depends on this behavior.
pub trait ZeroInitializedStats {
    /// `True` if we have have not emitted a "zero update" for this stats blob.
    fn needs_zero_initialization(&self) -> bool;

    /// Marks that we have emitted a "zero update" for this stats blob.
    fn mark_zero_initialized(&mut self);

    /// Returns a "all zero" instance of this stat, with collection id and/or
    /// replica id retained.
    fn zero_stat(&self) -> Self;
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
    // `offset_committed` is calculated from source frontiers, which never go backwards, so a
    // regression is a bug in how we calculate it and `incorporate` logs it as an error.
    //
    // `offset_known` comes from the upstream system, which can legitimately move it backwards,
    // for example when a Kafka topic is deleted and recreated or a Postgres database is restored
    // from a backup. `incorporate` reports that regression to the caller instead of logging it,
    // and callers count it on a metric. The source status system tables are where we attempt to
    // communicate these upstream events to the user.
    pub offset_known: Gauge<Total>,
    pub offset_committed: Gauge<Total>,
}

/// Gauges whose incoming value [`SourceStatisticsUpdate::incorporate`] refused because it was
/// lower than the value already held. Whether that is an error is the caller's decision.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct Regressions {
    pub offset_known: bool,
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

    pub fn incorporate(&mut self, other: SourceStatisticsUpdate) -> Regressions {
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
        offset_committed.incorporate(other.offset_committed, "offset_committed");

        Regressions {
            offset_known: offset_known.0.merge(other.offset_known.0).is_some(),
        }
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
        packer.push(Datum::from(self.records_indexed.0.0));
        packer.push(Datum::from(self.bytes_indexed.0.0));
        let rehydration_latency = self
            .rehydration_latency_ms
            .0
            .0
            .map(|ms| mz_repr::adt::interval::Interval::new(0, 0, ms * 1000));
        packer.push(Datum::from(rehydration_latency));
        packer.push(Datum::from(self.snapshot_records_known.0.0));
        packer.push(Datum::from(self.snapshot_records_staged.0.0));
        // Gauges
        packer.push(Datum::from(self.snapshot_committed.0.0));
        packer.push(Datum::from(self.offset_known.0.pack()));
        packer.push(Datum::from(self.offset_committed.0.pack()));
    }

    fn unpack(
        row: Row,
        _metrics: &crate::metrics::StorageControllerMetrics,
    ) -> (GlobalId, Option<ReplicaId>, Self) {
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
            offset_known: Gauge::gauge(Some(iter.next().unwrap().unwrap_uint64())),
            offset_committed: Gauge::gauge(Some(iter.next().unwrap().unwrap_uint64())),
        };

        (s.id, None, s)
    }
}

/// Statistics that we keep in the controller about a given collection
/// (identified by id) on a given replica (identified by replica_id).
///
/// This mirrors [SourceStatisticsUpdate] and we update ourselve by
/// incorporating them using [ControllerSourceStatistics::incorporate]
#[derive(Clone, Debug)]
pub struct ControllerSourceStatistics {
    pub id: GlobalId,

    // This is an Option because of Webhook "sources", which don't really run on
    // a cluster, but we decided a while ago that they are "sources", so here we
    // are and these show up in the source statistics collection.
    pub replica_id: Option<ReplicaId>,

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
    pub offset_known: Gauge<Total>,
    pub offset_committed: Gauge<Total>,

    // We don't try to be cute with updating and removing stats when dropping
    // replicas, which might run into problems with race conditions or when
    // crashes occur.
    //
    // Instead, we mark when a statistic was last updated and periodically prune
    // old statistics. It's fine if this is not 100% accurate or when the
    // "counter" resets when the controller restarts. Eventually things will get
    // dropped.
    pub last_updated: Instant,

    // For implementing `ZeroInitializedStats`.
    needs_zero_initialization: bool,

    /// Bumped each time this replica reports a lower `offset_known` than the one held. See the
    /// `offset_known` field of [`SourceStatisticsUpdate`] for why that is not an error.
    regressed_offset_known: RegressionCounter,
}

impl ControllerSourceStatistics {
    pub fn new(
        id: GlobalId,
        replica_id: Option<ReplicaId>,
        metrics: &crate::metrics::StorageControllerMetrics,
    ) -> Self {
        Self::zeroed(
            id,
            replica_id,
            metrics.regressed_offset_known(id, replica_id),
        )
    }

    fn zeroed(
        id: GlobalId,
        replica_id: Option<ReplicaId>,
        regressed_offset_known: RegressionCounter,
    ) -> Self {
        Self {
            id,
            replica_id,
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
            last_updated: Instant::now(),
            needs_zero_initialization: true,
            regressed_offset_known,
        }
    }

    /// Incorporate updates from the given [SourceStatisticsUpdate] into
    /// ourselves
    pub fn incorporate(&mut self, update: SourceStatisticsUpdate) {
        let ControllerSourceStatistics {
            id: _,
            replica_id: _,
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
            last_updated,
            needs_zero_initialization: _,
            regressed_offset_known,
        } = self;

        messages_received.incorporate(update.messages_received, "messages_received");
        bytes_received.incorporate(update.bytes_received, "bytes_received");
        updates_staged.incorporate(update.updates_staged, "updates_staged");
        updates_committed.incorporate(update.updates_committed, "updates_committed");
        records_indexed.incorporate(update.records_indexed, "records_indexed");
        bytes_indexed.incorporate(update.bytes_indexed, "bytes_indexed");
        rehydration_latency_ms.incorporate(update.rehydration_latency_ms, "rehydration_latency_ms");
        snapshot_records_known.incorporate(update.snapshot_records_known, "snapshot_records_known");
        snapshot_records_staged
            .incorporate(update.snapshot_records_staged, "snapshot_records_staged");
        snapshot_committed.incorporate(update.snapshot_committed, "snapshot_committed");
        offset_committed.incorporate(update.offset_committed, "offset_committed");
        if offset_known.0.merge(update.offset_known.0).is_some() {
            regressed_offset_known.inc();
        }

        *last_updated = Instant::now();
    }
}

impl PackableStats for ControllerSourceStatistics {
    fn pack(&self, mut packer: mz_repr::RowPacker<'_>) {
        use mz_repr::Datum;
        // id
        packer.push(Datum::from(self.id.to_string().as_str()));
        // replica_id
        if let Some(replica_id) = self.replica_id {
            packer.push(Datum::from(replica_id.to_string().as_str()));
        } else {
            packer.push(Datum::Null);
        }
        // Counters.
        packer.push(Datum::from(self.messages_received.0));
        packer.push(Datum::from(self.bytes_received.0));
        packer.push(Datum::from(self.updates_staged.0));
        packer.push(Datum::from(self.updates_committed.0));
        // Resetting gauges.
        packer.push(Datum::from(self.records_indexed.0.0));
        packer.push(Datum::from(self.bytes_indexed.0.0));
        let rehydration_latency = self
            .rehydration_latency_ms
            .0
            .0
            .map(|ms| mz_repr::adt::interval::Interval::new(0, 0, ms * 1000));
        packer.push(Datum::from(rehydration_latency));
        packer.push(Datum::from(self.snapshot_records_known.0.0));
        packer.push(Datum::from(self.snapshot_records_staged.0.0));
        // Gauges
        packer.push(Datum::from(self.snapshot_committed.0.0));
        packer.push(Datum::from(self.offset_known.0.pack()));
        packer.push(Datum::from(self.offset_committed.0.pack()));
    }

    fn unpack(
        row: Row,
        metrics: &crate::metrics::StorageControllerMetrics,
    ) -> (GlobalId, Option<ReplicaId>, Self) {
        let mut iter = row.iter();
        let id = iter.next().unwrap().unwrap_str().parse().unwrap();
        let replica_id_or_null = iter.next().unwrap();

        let replica_id = if replica_id_or_null.is_null() {
            None
        } else {
            Some(
                replica_id_or_null
                    .unwrap_str()
                    .parse::<ReplicaId>()
                    .unwrap(),
            )
        };

        let s = Self {
            id,
            replica_id,

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
            last_updated: Instant::now(),
            // This is coming from a Row, so we have definitely already written
            // out a "zero update" at some point.
            needs_zero_initialization: false,
            regressed_offset_known: metrics.regressed_offset_known(id, replica_id),
        };

        (s.id, replica_id, s)
    }
}

impl ExpirableStats for ControllerSourceStatistics {
    fn last_updated(&self) -> Instant {
        self.last_updated
    }
}

impl ZeroInitializedStats for ControllerSourceStatistics {
    fn needs_zero_initialization(&self) -> bool {
        self.needs_zero_initialization
    }

    fn mark_zero_initialized(&mut self) {
        self.needs_zero_initialization = false;
    }

    fn zero_stat(&self) -> Self {
        Self::zeroed(
            self.id,
            self.replica_id,
            self.regressed_offset_known.clone(),
        )
    }
}

/// Statistics that we keep in the controller about a given collection
/// (identified by id) on a given replica (identified by replica_id).
///
/// This mirrors [SinkStatisticsUpdate] and we update ourselve by incorporating
/// them using [ControllerSinkStatistics::incorporate]
#[derive(Clone, Debug, PartialEq)]
pub struct ControllerSinkStatistics {
    pub id: GlobalId,
    pub replica_id: ReplicaId,

    pub messages_staged: Counter,
    pub messages_committed: Counter,
    pub bytes_staged: Counter,
    pub bytes_committed: Counter,

    // See comment on ControllerSourceStatistics.last_updated.
    pub last_updated: Instant,

    // For implementing `ZeroInitializedStats`.
    needs_zero_initialization: bool,
}

impl ControllerSinkStatistics {
    pub fn new(id: GlobalId, replica_id: ReplicaId) -> Self {
        Self {
            id,
            replica_id,
            messages_staged: Default::default(),
            messages_committed: Default::default(),
            bytes_staged: Default::default(),
            bytes_committed: Default::default(),
            last_updated: Instant::now(),
            needs_zero_initialization: true,
        }
    }

    /// Incorporate updates from the given [SinkStatisticsUpdate] into ourselves
    pub fn incorporate(&mut self, update: SinkStatisticsUpdate) {
        let ControllerSinkStatistics {
            id: _,
            replica_id: _,
            messages_staged,
            messages_committed,
            bytes_staged,
            bytes_committed,
            last_updated,
            needs_zero_initialization: _,
        } = self;

        messages_staged.incorporate(update.messages_staged, "messages_staged");
        bytes_staged.incorporate(update.bytes_staged, "bytes_staged");
        messages_committed.incorporate(update.messages_committed, "messages_committed");
        bytes_committed.incorporate(update.bytes_committed, "bytes_committed");

        *last_updated = Instant::now();
    }
}

impl PackableStats for ControllerSinkStatistics {
    fn pack(&self, mut packer: mz_repr::RowPacker<'_>) {
        use mz_repr::Datum;
        packer.push(Datum::from(self.id.to_string().as_str()));
        packer.push(Datum::from(self.replica_id.to_string().as_str()));
        packer.push(Datum::from(self.messages_staged.0));
        packer.push(Datum::from(self.messages_committed.0));
        packer.push(Datum::from(self.bytes_staged.0));
        packer.push(Datum::from(self.bytes_committed.0));
    }

    fn unpack(
        row: Row,
        _metrics: &crate::metrics::StorageControllerMetrics,
    ) -> (GlobalId, Option<ReplicaId>, Self) {
        let mut iter = row.iter();
        let s = Self {
            id: iter.next().unwrap().unwrap_str().parse().unwrap(),
            replica_id: iter.next().unwrap().unwrap_str().parse().unwrap(),
            messages_staged: iter.next().unwrap().unwrap_uint64().into(),
            messages_committed: iter.next().unwrap().unwrap_uint64().into(),
            bytes_staged: iter.next().unwrap().unwrap_uint64().into(),
            bytes_committed: iter.next().unwrap().unwrap_uint64().into(),
            last_updated: Instant::now(),
            // This is coming from a Row, so we have definitely already written
            // out a "zero update" at some point.
            needs_zero_initialization: false,
        };
        (s.id, Some(s.replica_id), s)
    }
}

impl ExpirableStats for ControllerSinkStatistics {
    fn last_updated(&self) -> Instant {
        self.last_updated
    }
}

impl ZeroInitializedStats for ControllerSinkStatistics {
    fn needs_zero_initialization(&self) -> bool {
        self.needs_zero_initialization
    }

    fn mark_zero_initialized(&mut self) {
        self.needs_zero_initialization = false;
    }

    fn zero_stat(&self) -> Self {
        ControllerSinkStatistics::new(self.id, self.replica_id)
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

    fn unpack(
        row: Row,
        _metrics: &crate::metrics::StorageControllerMetrics,
    ) -> (GlobalId, Option<ReplicaId>, Self) {
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
        (s.id, None, s)
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

#[cfg(test)]
mod tests {
    use mz_cluster_client::metrics::ControllerMetrics;
    use mz_ore::metrics::MetricsRegistry;

    use super::*;

    #[mz_ore::test]
    fn offset_known_regression_is_counted() {
        let registry = MetricsRegistry::new();
        let metrics = crate::metrics::StorageControllerMetrics::new(
            &registry,
            ControllerMetrics::new(&registry),
        );
        let id = GlobalId::User(1);
        let update = |offset_known: u64| SourceStatisticsUpdate {
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
            offset_known: Gauge::gauge(Some(offset_known)),
            offset_committed: Default::default(),
        };

        let mut stats = ControllerSourceStatistics::new(id, None, &metrics);
        stats.incorporate(update(10));
        stats.incorporate(update(5));

        assert_eq!(stats.offset_known.0.total, Some(10));
        assert_eq!(stats.regressed_offset_known.get(), 1);
    }

    #[mz_ore::test]
    fn source_update_reports_offset_known_regression() {
        let id = GlobalId::User(1);
        let update = |offset_known: u64, offset_committed: u64| SourceStatisticsUpdate {
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
            offset_known: Gauge::gauge(Some(offset_known)),
            offset_committed: Gauge::gauge(Some(offset_committed)),
        };

        let mut stats = update(10, 10);
        assert_eq!(
            stats.incorporate(update(12, 12)),
            Regressions {
                offset_known: false
            }
        );
        assert_eq!(
            stats.incorporate(update(5, 12)),
            Regressions { offset_known: true }
        );
        assert_eq!(stats.offset_known.0.total, Some(12));
    }
}

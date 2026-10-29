// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Recording of wallclock freshness histograms for compute collections.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use mz_controller_types::dyncfgs::{
    ENABLE_WALLCLOCK_FRESHNESS_RECORDING, WALLCLOCK_FRESHNESS_BUCKETS_PER_OCTAVE,
    WALLCLOCK_FRESHNESS_PERIOD_INTERVAL, WALLCLOCK_FRESHNESS_VIRTUAL_ARRIVAL_INTERVAL,
};
use mz_dyncfg::ConfigSet;
use mz_ore::soft_panic_or_log;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_storage_types::dyncfgs::WALLCLOCK_FRESHNESS_HISTOGRAM_RETENTION_INTERVAL;
use mz_storage_types::freshness::ResolvedTree;
use mz_storage_types::freshness::histogram::BucketScheme;
use mz_storage_types::freshness::recorder::{FreshnessRecorder, PeriodRow, RecorderConfig};
use timely::progress::Antichain;

use crate::controller::ReplicaId;

/// The `measure` of rows that count the delay from arrival to visibility in the output.
const END_TO_END: &str = "end_to_end";

/// Where a recording starts, consulted until it does.
#[derive(Debug)]
enum Start {
    /// At the latest of what the as-of reflects, the wall-clock creation time in ms, and the
    /// retention before the first observation.
    Created(Antichain<Timestamp>, u64),
    /// At the later of what the first observed frontier reflects and the wall-clock time in ms
    /// at which the recording was built.
    FirstObservation(u64),
}

/// Freshness recording of one write frontier, the global one of a collection or a replica's.
#[derive(Debug)]
pub(super) struct FreshnessRecording {
    start: Start,
    end_to_end: Option<FreshnessRecorder>,
}

impl FreshnessRecording {
    /// Records the arrivals after the later of what `as_of` reflects and `created_ms`, so a new
    /// collection's hydration counts as latency.
    ///
    /// Arrivals at or before `created_ms` predate the collection, and arrivals at or before what
    /// the as-of reflects are not in its output, so neither is recorded. An as-of that reflects
    /// nothing, as `[0]` or a refresh view's before its first refresh, starts at `created_ms`.
    pub fn created(as_of: Antichain<Timestamp>, created_ms: u64) -> Self {
        FreshnessRecording {
            start: Start::Created(as_of, created_ms),
            end_to_end: None,
        }
    }

    /// Records the arrivals after the later of the first observed frontier and `built_ms`, the
    /// wall-clock time now. The arrivals before it are not recorded, which leaves a hole in the
    /// histogram.
    pub fn at_first_observation(built_ms: u64) -> Self {
        FreshnessRecording {
            start: Start::FirstObservation(built_ms),
            end_to_end: None,
        }
    }

    /// Records a write frontier observed at `now_ms` and returns the rows of closed periods.
    pub fn observe(
        &mut self,
        tree: &ResolvedTree,
        frontier: &Antichain<Timestamp>,
        now_ms: u64,
        config: &RecorderConfig,
    ) -> Vec<PeriodRow> {
        let reflected =
            |f: &Antichain<Timestamp>| f.as_option().and_then(|t| tree.last_reflected(*t));
        if self.end_to_end.is_none() {
            let start = match &self.start {
                Start::Created(as_of, created_ms) => {
                    let retained = now_ms.saturating_sub(config.retention_ms);
                    let reflected = reflected(as_of).unwrap_or(0);
                    Some(reflected.max(*created_ms).max(retained))
                }
                // A first frontier near the epoch, as of a collection whose as-of is `[0]`, would
                // otherwise charge the time since the epoch as latency.
                Start::FirstObservation(built_ms) => {
                    reflected(frontier).map(|first| first.max(*built_ms))
                }
            };
            let Some(start) = start else {
                return Vec::new();
            };
            self.end_to_end = Some(FreshnessRecorder::new(start));
        }
        let recorder = self.end_to_end.as_mut().expect("initialized above");
        recorder.observe(tree, frontier, now_ms, config)
    }

    /// Ends the recording of a collection that goes away and returns the rows of its open
    /// periods, which hold the arrivals up to the last observed frontier.
    pub fn finish(&mut self) -> Vec<PeriodRow> {
        self.end_to_end
            .as_mut()
            .map(FreshnessRecorder::finish)
            .unwrap_or_default()
    }
}

/// The recorder configuration, `None` if recording is disabled.
pub(super) fn recorder_config(dyncfg: &ConfigSet) -> Option<RecorderConfig> {
    if !ENABLE_WALLCLOCK_FRESHNESS_RECORDING.get(dyncfg) {
        return None;
    }
    let per_octave = WALLCLOCK_FRESHNESS_BUCKETS_PER_OCTAVE.get(dyncfg);
    let scheme = BucketScheme::new(per_octave).unwrap_or_else(|| {
        soft_panic_or_log!("invalid wallclock freshness buckets per octave: {per_octave}");
        let default = *WALLCLOCK_FRESHNESS_BUCKETS_PER_OCTAVE.default();
        BucketScheme::new(default).expect("valid default")
    });
    Some(RecorderConfig {
        scheme,
        interval_ms: millis(WALLCLOCK_FRESHNESS_VIRTUAL_ARRIVAL_INTERVAL.get(dyncfg)),
        period_ms: millis(WALLCLOCK_FRESHNESS_PERIOD_INTERVAL.get(dyncfg)),
        retention_ms: millis(WALLCLOCK_FRESHNESS_HISTOGRAM_RETENTION_INTERVAL.get(dyncfg)),
    })
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Packs `rows` of a collection's global histogram, or of a replica's if `replica_id` is set.
pub(super) fn pack_rows(
    rows: Vec<PeriodRow>,
    collection_id: GlobalId,
    replica_id: Option<ReplicaId>,
    labels: &BTreeMap<&'static str, String>,
) -> Vec<(Row, Diff)> {
    let collection_id = collection_id.to_string();
    let replica_id = replica_id.map(|id| id.to_string());
    let mut row_buf = Row::default();
    let mut updates = Vec::with_capacity(rows.len());
    for row in rows {
        let (Some(start), Some(end)) =
            (timestamp(row.period_start_ms), timestamp(row.period_end_ms))
        else {
            soft_panic_or_log!(
                "freshness period out of range (collection_id={collection_id}, row={row:?})"
            );
            continue;
        };
        let mut packer = row_buf.packer();
        packer.extend([
            Datum::TimestampTz(start),
            Datum::TimestampTz(end),
            Datum::String(&collection_id),
        ]);
        if let Some(replica_id) = &replica_id {
            packer.push(Datum::String(replica_id));
        }
        packer.extend([
            Datum::String(END_TO_END),
            Datum::UInt32(row.buckets_per_octave),
            Datum::Int32(row.bucket),
        ]);
        packer.push_dict(labels.iter().map(|(k, v)| (*k, Datum::String(v))));
        let count = i64::try_from(row.count).unwrap_or(i64::MAX);
        updates.push((row_buf.clone(), Diff::from(count)));
    }
    updates
}

fn timestamp(ms: u64) -> Option<CheckedTimestamp<DateTime<Utc>>> {
    let datetime = DateTime::from_timestamp_millis(i64::try_from(ms).ok()?)?;
    CheckedTimestamp::try_from(datetime).ok()
}

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
use mz_storage_types::freshness::ResolvedTree;
use mz_storage_types::freshness::histogram::BucketScheme;
use mz_storage_types::freshness::recorder::{FreshnessRecorder, PeriodRow, RecorderConfig};
use timely::progress::Antichain;

use crate::controller::ReplicaId;

/// The `measure` of rows that count the delay from arrival to visibility in the output.
const END_TO_END: &str = "end_to_end";

/// Freshness recording of one write frontier, the global one of a collection or a replica's.
#[derive(Debug)]
pub(super) struct FreshnessRecording {
    /// The as-of and wall-clock creation time in ms of a collection whose recording starts at its
    /// creation. `None` if the recording starts at the first observed frontier.
    created: Option<(Antichain<Timestamp>, u64)>,
    end_to_end: Option<FreshnessRecorder>,
}

impl FreshnessRecording {
    /// Records the arrivals after the later of what `as_of` reflects and `created_ms`, so a new
    /// collection's hydration counts as latency.
    pub fn created(as_of: Antichain<Timestamp>, created_ms: u64) -> Self {
        FreshnessRecording {
            created: Some((as_of, created_ms)),
            end_to_end: None,
        }
    }

    /// Records the arrivals after the first observed frontier. The arrivals before it are not
    /// recorded, which leaves a hole in the histogram.
    pub fn at_first_observation() -> Self {
        FreshnessRecording {
            created: None,
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
            // If nothing of the as-of is reflected, as for a refresh view before its first
            // refresh, the first observation that reflects something starts the recording.
            let start = self
                .created
                .as_ref()
                .and_then(|(as_of, created_ms)| Some(reflected(as_of)?.max(*created_ms)))
                .or_else(|| reflected(frontier));
            let Some(start) = start else {
                return Vec::new();
            };
            self.end_to_end = Some(FreshnessRecorder::new(start));
        }
        let recorder = self.end_to_end.as_mut().expect("initialized above");
        recorder.observe(tree, frontier, now_ms, config)
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

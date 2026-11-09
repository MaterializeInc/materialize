// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Wall-clock staleness state of a frontier, shared by the compute and storage controllers.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use mz_cluster_client::ReplicaId;
use mz_controller_types::dyncfgs::{
    ENABLE_WALLCLOCK_FRESHNESS_RECORDING, WALLCLOCK_FRESHNESS_BUCKETS_PER_OCTAVE,
    WALLCLOCK_FRESHNESS_PERIOD_INTERVAL,
};
use mz_dyncfg::ConfigSet;
use mz_ore::soft_panic_or_log;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_storage_types::freshness::{
    BucketScheme, PeriodRow, RecorderConfig, StalenessRecorder, StalenessReporter, reflected_time,
};
use mz_storage_types::time_dependence::TimeDependence;
use timely::progress::Antichain;

/// The shortest period interval. Every recorded collection writes rows for every period, so
/// shorter configured intervals count as this one.
pub const MIN_PERIOD_INTERVAL: Duration = Duration::from_secs(1);

/// The last rejected buckets per octave, `u64::MAX` if none.
static REJECTED_BUCKETS_PER_OCTAVE: AtomicU64 = AtomicU64::new(u64::MAX);
/// The last rejected period interval in milliseconds, `u64::MAX` if none.
static REJECTED_PERIOD_MS: AtomicU64 = AtomicU64::new(u64::MAX);

/// The recorder configuration, `None` if recording is disabled.
///
/// A buckets per octave that is not a valid [`BucketScheme`] counts as the default, and a
/// period interval below [`MIN_PERIOD_INTERVAL`] counts as that minimum.
pub fn recorder_config(dyncfg: &ConfigSet) -> Option<RecorderConfig> {
    if !ENABLE_WALLCLOCK_FRESHNESS_RECORDING.get(dyncfg) {
        return None;
    }
    let per_octave = WALLCLOCK_FRESHNESS_BUCKETS_PER_OCTAVE.get(dyncfg);
    let scheme = BucketScheme::new(per_octave).unwrap_or_else(|| {
        let default = *WALLCLOCK_FRESHNESS_BUCKETS_PER_OCTAVE.default();
        if is_new_rejection(&REJECTED_BUCKETS_PER_OCTAVE, u64::from(per_octave)) {
            tracing::warn!(
                "invalid wallclock freshness buckets per octave {per_octave}, using {default}"
            );
        }
        BucketScheme::new(default).expect("valid default")
    });
    let mut period = WALLCLOCK_FRESHNESS_PERIOD_INTERVAL.get(dyncfg);
    if period < MIN_PERIOD_INTERVAL {
        let period_ms = u64::try_from(period.as_millis()).expect("below the minimum");
        if is_new_rejection(&REJECTED_PERIOD_MS, period_ms) {
            tracing::warn!(
                "wallclock freshness period interval {period:?} below the minimum, using \
                {MIN_PERIOD_INTERVAL:?}"
            );
        }
        period = MIN_PERIOD_INTERVAL;
    }
    Some(RecorderConfig { scheme, period })
}

/// Records `value` as the last one rejected in `last`, and returns whether it differs from the
/// previous one.
///
/// Both controllers read the configuration on every frontier response, so logging every
/// rejection rather than only new ones would flood the log.
fn is_new_rejection(last: &AtomicU64, value: u64) -> bool {
    last.swap(value, Ordering::Relaxed) != value
}

/// Packs `rows` of a collection's global histogram, or of a replica's if `replica_id` is set.
pub fn pack_rows(
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
        let (Some(start), Some(end)) = (timestamp(row.period_start), timestamp(row.period_end))
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
            Datum::String("end_to_end"),
            Datum::UInt32(row.buckets_per_octave),
            Datum::Int32(row.bucket),
        ]);
        packer.push_dict(labels.iter().map(|(k, v)| (*k, Datum::String(v))));
        let count = i64::try_from(row.count_ms).unwrap_or(i64::MAX);
        updates.push((row_buf.clone(), Diff::from(count)));
    }
    updates
}

fn timestamp(t: Timestamp) -> Option<CheckedTimestamp<DateTime<Utc>>> {
    let datetime = DateTime::from_timestamp_millis(i64::try_from(u64::from(t)).ok()?)?;
    CheckedTimestamp::try_from(datetime).ok()
}

/// The wall-clock staleness of one frontier of a collection, the global one or a replica's.
///
/// It always reports staleness, and records histograms while it is passed a recorder
/// configuration. A frontier that reflects no time yet reflects the start of the reporter
/// or recording it feeds. Once the frontier is empty, or after [`FreshnessState::finish`], the
/// state is complete: it records nothing more and reports zero staleness.
#[derive(Debug)]
pub struct FreshnessState {
    dependence: TimeDependence,
    start: Timestamp,
    /// The latest observed frontier.
    frontier: Antichain<Timestamp>,
    /// The reflected time of the latest frontier, `None` if it reflects no time.
    reflected: Option<Timestamp>,
    reporter: StalenessReporter,
    /// The recorder and the time it started.
    recorder: Option<(Timestamp, StalenessRecorder)>,
    complete: bool,
}

impl FreshnessState {
    /// Creates the state of a frontier observed first at `start`, with the freshness
    /// `dependence` of its collection.
    pub fn new(
        dependence: TimeDependence,
        start: Timestamp,
        frontier: &Antichain<Timestamp>,
        config: Option<&RecorderConfig>,
    ) -> Self {
        let reflected = reflect(&dependence, frontier);
        let mut state = FreshnessState {
            dependence,
            start,
            frontier: frontier.clone(),
            reflected,
            reporter: StalenessReporter::new(start, reflected.unwrap_or(start)),
            recorder: None,
            complete: frontier.is_empty(),
        };
        state.apply_config(start, config);
        state
    }

    /// Records that the frontier is `frontier` at `now`, and returns the periods that closed.
    ///
    /// Only a changed frontier closes the open sample. Observing the latest frontier again
    /// leaves the sample open, so a stall is one sample per period.
    pub fn observe(
        &mut self,
        now: Timestamp,
        frontier: &Antichain<Timestamp>,
        config: Option<&RecorderConfig>,
    ) -> Vec<PeriodRow> {
        if frontier.is_empty() {
            return self.finish(now, config);
        }
        if self.complete {
            return Vec::new();
        }
        self.apply_config(now, config);
        // Closing the sample at a repeated frontier would split a stall into samples charged
        // at their own, smaller, staleness.
        if *frontier == self.frontier {
            return Vec::new();
        }
        self.frontier.clone_from(frontier);
        self.reflected = reflect(&self.dependence, frontier);
        self.reporter
            .observe(now, self.reflected.unwrap_or(self.start));
        match (&mut self.recorder, config) {
            (Some((start, recorder)), Some(config)) => {
                recorder.observe(now, self.reflected.unwrap_or(*start), config)
            }
            _ => Vec::new(),
        }
    }

    /// Closes the periods that ended at or before `now`, and returns them.
    pub fn tick(&mut self, now: Timestamp, config: Option<&RecorderConfig>) -> Vec<PeriodRow> {
        if self.complete {
            return Vec::new();
        }
        self.apply_config(now, config);
        match (&mut self.recorder, config) {
            (Some((_, recorder)), Some(config)) => recorder.tick(now, config),
            _ => Vec::new(),
        }
    }

    /// Ends the recording at `now`, and returns the remaining periods, including the open
    /// partial one.
    pub fn finish(&mut self, now: Timestamp, config: Option<&RecorderConfig>) -> Vec<PeriodRow> {
        if self.complete {
            return Vec::new();
        }
        self.apply_config(now, config);
        self.complete = true;
        match (self.recorder.take(), config) {
            (Some((_, mut recorder)), Some(config)) => recorder.finish(now, config),
            _ => Vec::new(),
        }
    }

    /// The staleness to report at `now`, see [`StalenessReporter::read`].
    pub fn reported_lag(&mut self, now: Timestamp) -> Duration {
        if self.complete {
            return Duration::ZERO;
        }
        self.reporter.read(now)
    }

    /// Creates or drops the recorder to match `config`.
    fn apply_config(&mut self, now: Timestamp, config: Option<&RecorderConfig>) {
        if self.complete {
            return;
        }
        match (&self.recorder, config) {
            // A recorder created after construction starts at `now`, so that no sample spans
            // time the flag was off.
            (None, Some(_)) => {
                let recorder = StalenessRecorder::new(now, self.reflected.unwrap_or(now));
                self.recorder = Some((now, recorder));
            }
            // The flag was turned off, so the open period is dropped without writing it.
            (Some(_), None) => self.recorder = None,
            (Some(_), Some(_)) | (None, None) => {}
        }
    }
}

/// The reflected time of `frontier`, `None` if it reflects no time.
fn reflect(dependence: &TimeDependence, frontier: &Antichain<Timestamp>) -> Option<Timestamp> {
    frontier
        .as_option()
        .and_then(|frontier| reflected_time(dependence, *frontier))
}

#[cfg(test)]
mod tests;

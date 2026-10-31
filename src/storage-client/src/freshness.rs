// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Recording of wallclock freshness histograms, shared by the compute and storage controllers.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use mz_cluster_client::ReplicaId;
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
use mz_storage_types::freshness::local::{
    LocalDelayRecorder, LocalInput, LocalRows, VisibilityLog,
};
use mz_storage_types::freshness::recorder::{FreshnessRecorder, PeriodRow, RecorderConfig};
use timely::progress::Antichain;

use crate::controller::WallclockLag;

/// What a freshness histogram row measures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Measure {
    /// The delay from arrival to visibility in the output.
    EndToEnd,
    /// The delay from every direct input's frontier passing the timestamp the output needs for
    /// an arrival to the arrival's visibility in the output.
    Local,
    /// The delay from visibility in the slowest direct input to every direct input's frontier
    /// passing the timestamp the output needs, at most the wait the inputs' tick and refresh
    /// schedules cause when they are on time. Recorded only for outputs with two or more direct
    /// inputs.
    Skew,
}

impl Measure {
    /// The value of the `measure` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            Measure::EndToEnd => "end_to_end",
            Measure::Local => "local",
            Measure::Skew => "skew",
        }
    }
}

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
pub struct FreshnessRecording {
    start: Start,
    /// Whether the local delay and skew are recorded, see [`Self::end_to_end_only`].
    local: bool,
    /// The recorders of all recorded measures. They start together, so their per-period counts
    /// agree.
    recorders: Option<(FreshnessRecorder, Option<LocalDelayRecorder>)>,
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
            local: true,
            recorders: None,
        }
    }

    /// Records the arrivals after the later of the first observed frontier and `built_ms`, the
    /// wall-clock time now. The arrivals before it are not recorded, which leaves a hole in the
    /// histogram.
    pub fn at_first_observation(built_ms: u64) -> Self {
        FreshnessRecording {
            start: Start::FirstObservation(built_ms),
            local: true,
            recorders: None,
        }
    }

    /// Records only the end-to-end latency, for a collection without inputs inside Materialize,
    /// whose local delay is its end-to-end latency.
    pub fn end_to_end_only(mut self) -> Self {
        self.local = false;
        self
    }

    fn recorders(&self, start_ms: u64) -> (FreshnessRecorder, Option<LocalDelayRecorder>) {
        (
            FreshnessRecorder::new(start_ms),
            self.local.then(|| LocalDelayRecorder::new(start_ms)),
        )
    }

    /// The last arrival recorded so far, `None` before the recording starts. The input logs
    /// passed to [`Self::observe`] need to answer only the arrivals above it.
    pub fn recorded_through(&self) -> Option<u64> {
        let (end_to_end, local) = self.recorders.as_ref()?;
        Some(match local {
            Some(local) => local.high_water_ms(),
            None => end_to_end.high_water_ms(),
        })
    }

    /// Records a write frontier observed at `now_ms` and returns the rows of closed periods of
    /// all measures.
    ///
    /// `needed` and `inputs`, the visibility logs of the direct inputs, are as for
    /// [`LocalDelayRecorder::observe`].
    pub fn observe(
        &mut self,
        tree: &ResolvedTree,
        needed: &ResolvedTree,
        frontier: &Antichain<Timestamp>,
        now_ms: u64,
        inputs: &[LocalInput<'_>],
        config: &RecorderConfig,
    ) -> Vec<(Measure, PeriodRow)> {
        let reflected =
            |f: &Antichain<Timestamp>| f.as_option().and_then(|t| tree.last_reflected(*t));
        if self.recorders.is_none() {
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
            self.recorders = Some(self.recorders(start));
        }
        let (end_to_end, local) = self.recorders.as_mut().expect("initialized above");
        let end_to_end = end_to_end.observe(tree, frontier, now_ms, config);
        let local = local
            .as_mut()
            .map(|local| local.observe(tree, needed, frontier, now_ms, inputs, config))
            .unwrap_or_default();
        tag(end_to_end, local)
    }

    /// Ends the recording of a collection that goes away and returns the rows of the open
    /// periods of all measures, which hold the arrivals up to the last observed frontier.
    pub fn finish(&mut self) -> Vec<(Measure, PeriodRow)> {
        let Some((end_to_end, local)) = self.recorders.as_mut() else {
            return Vec::new();
        };
        let local = local
            .as_mut()
            .map(LocalDelayRecorder::finish)
            .unwrap_or_default();
        tag(end_to_end.finish(), local)
    }
}

/// The rows of all measures, tagged with their measure.
fn tag(end_to_end: Vec<PeriodRow>, local: LocalRows) -> Vec<(Measure, PeriodRow)> {
    let end_to_end = end_to_end.into_iter().map(|row| (Measure::EndToEnd, row));
    let skew = local.skew.into_iter().map(|row| (Measure::Skew, row));
    let local = local.local.into_iter().map(|row| (Measure::Local, row));
    end_to_end.chain(local).chain(skew).collect()
}

/// Records in `log` that `frontier`, and the arrivals it reflects, were visible at `now_ms`, and
/// drops the entries older than the retention of `config`.
pub fn record_visibility(
    log: &mut VisibilityLog,
    tree: &ResolvedTree,
    frontier: &Antichain<Timestamp>,
    now_ms: u64,
    config: &RecorderConfig,
) {
    let (frontier, reflected) = match frontier.as_option() {
        Some(t) => (u64::from(*t), tree.last_reflected(*t)),
        // A complete collection makes every arrival visible from now on. Without this entry its
        // consumers would find no answer for later arrivals and record a local delay of 0.
        None => (u64::MAX, Some(u64::MAX)),
    };
    log.record(frontier, reflected, now_ms);
    // Pruned by age, not through the least arrival its consumers recorded, because consumers in
    // other controllers read the log and tracking them would need a registration protocol. A
    // consumer that lags the input by more than the retention finds those arrivals visible when
    // the last pruned entry was observed, which understates and never overstates.
    log.prune_observed_before(now_ms.saturating_sub(visibility_retention_ms(config)));
}

/// How long visibility log entries are kept: ten periods, at least ten minutes.
fn visibility_retention_ms(config: &RecorderConfig) -> u64 {
    config.period_ms.saturating_mul(10).max(600_000)
}

/// A copy of `log` that answers the arrivals above `recorded_through`.
pub fn input_log(log: &VisibilityLog, recorded_through: Option<u64>) -> VisibilityLog {
    log.after(recorded_through.unwrap_or(0))
}

/// Caches the assignment bound D of a collection's freshness tree.
#[derive(Debug, Default)]
pub struct AssignmentBoundCache {
    /// The tree D was last computed for, and D. Comparing whole trees keys the cache on the
    /// leaf tickers and the refresh schedules.
    cached: Option<(ResolvedTree, Option<u64>)>,
}

impl AssignmentBoundCache {
    /// Whether D has been computed for some tree.
    pub fn is_computed(&self) -> bool {
        self.cached.is_some()
    }

    /// [`ResolvedTree::assignment_bound`] of `tree`.
    pub fn get(&mut self, tree: &ResolvedTree) -> Option<u64> {
        match &self.cached {
            Some((cached, bound)) if cached == tree => *bound,
            // The scan behind D can take about a million evaluations, so it only runs when the
            // tree changes, for example after `ALTER SOURCE` changed a tick interval.
            _ => {
                let bound = tree.assignment_bound();
                self.cached = Some((tree.clone(), bound));
                bound
            }
        }
    }
}

/// The wallclock lag to report for a readable write frontier `frontier` at `now_ms`.
///
/// `freshness` is the collection's resolved freshness tree and its bounded D, `None` if
/// reporting is off or there is no such tree. `today` is the lag of `frontier` rounded up to
/// whole seconds, as `WallclockLagFn` computes it.
///
/// With `freshness`, the lag is max(D, S) in ms, where S is `now_ms` minus
/// `last_reflected(frontier)`. It is `today` without `freshness` or if no arrival is reflected
/// yet, because then there is no S to report.
pub fn reported_lag(
    freshness: Option<(&ResolvedTree, u64)>,
    frontier: &Antichain<Timestamp>,
    now_ms: u64,
    today: Duration,
) -> WallclockLag {
    let lag = freshness.and_then(|(tree, bound)| {
        // An empty frontier is complete forever, which `today` reports as 0.
        let reflected = tree.last_reflected(*frontier.as_option()?)?;
        Some(bound.max(now_ms.saturating_sub(reflected)))
    });
    lag.map_or_else(|| WallclockLag::from_duration(today), WallclockLag::Millis)
}

/// A visibility log shared between the controller that records it and the controllers whose
/// collections read it as an input.
#[derive(Clone, Debug, Default)]
pub struct SharedVisibilityLog(Arc<Mutex<VisibilityLog>>);

impl SharedVisibilityLog {
    /// Like [`record_visibility`].
    pub fn record(
        &self,
        tree: &ResolvedTree,
        frontier: &Antichain<Timestamp>,
        now_ms: u64,
        config: &RecorderConfig,
    ) {
        let mut log = self.0.lock().expect("lock poisoned");
        record_visibility(&mut log, tree, frontier, now_ms, config);
    }

    /// Like [`input_log`].
    pub fn input_log(&self, recorded_through: Option<u64>) -> VisibilityLog {
        input_log(&self.0.lock().expect("lock poisoned"), recorded_through)
    }
}

/// The recorder configuration, `None` if recording is disabled.
pub fn recorder_config(dyncfg: &ConfigSet) -> Option<RecorderConfig> {
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
pub fn pack_rows(
    rows: Vec<(Measure, PeriodRow)>,
    collection_id: GlobalId,
    replica_id: Option<ReplicaId>,
    labels: &BTreeMap<&'static str, String>,
) -> Vec<(Row, Diff)> {
    let collection_id = collection_id.to_string();
    let replica_id = replica_id.map(|id| id.to_string());
    let mut row_buf = Row::default();
    let mut updates = Vec::with_capacity(rows.len());
    for (measure, row) in rows {
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
            Datum::String(measure.as_str()),
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

#[cfg(test)]
mod tests;

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
use mz_storage_types::freshness::local::{LocalDelayRecorder, VisibilityLog};
use mz_storage_types::freshness::recorder::{FreshnessRecorder, PeriodRow, RecorderConfig};
use timely::progress::Antichain;

/// What a freshness histogram row measures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Measure {
    /// The delay from arrival to visibility in the output.
    EndToEnd,
    /// The delay from visibility in the slowest direct input to visibility in the output.
    Local,
}

impl Measure {
    /// The value of the `measure` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            Measure::EndToEnd => "end_to_end",
            Measure::Local => "local",
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
    /// The recorders of both measures. They start together, so their per-period counts agree.
    recorders: Option<(FreshnessRecorder, LocalDelayRecorder)>,
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
            recorders: None,
        }
    }

    /// Records the arrivals after the later of the first observed frontier and `built_ms`, the
    /// wall-clock time now. The arrivals before it are not recorded, which leaves a hole in the
    /// histogram.
    pub fn at_first_observation(built_ms: u64) -> Self {
        FreshnessRecording {
            start: Start::FirstObservation(built_ms),
            recorders: None,
        }
    }

    fn recorders(start_ms: u64) -> (FreshnessRecorder, LocalDelayRecorder) {
        (
            FreshnessRecorder::new(start_ms),
            LocalDelayRecorder::new(start_ms),
        )
    }

    /// The last arrival recorded so far, `None` before the recording starts. The input logs
    /// passed to [`Self::observe`] need to answer only the arrivals above it.
    pub fn recorded_through(&self) -> Option<u64> {
        let (_, local) = self.recorders.as_ref()?;
        Some(local.high_water_ms())
    }

    /// Records a write frontier observed at `now_ms` and returns the rows of closed periods of
    /// both measures.
    ///
    /// `inputs` are the visibility logs of the direct inputs, as for
    /// [`LocalDelayRecorder::observe`].
    pub fn observe(
        &mut self,
        tree: &ResolvedTree,
        frontier: &Antichain<Timestamp>,
        now_ms: u64,
        inputs: &[&VisibilityLog],
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
            self.recorders = Some(Self::recorders(start));
        }
        let (end_to_end, local) = self.recorders.as_mut().expect("initialized above");
        let end_to_end = end_to_end.observe(tree, frontier, now_ms, config);
        let local = local.observe(tree, frontier, now_ms, inputs, config);
        let end_to_end = end_to_end.into_iter().map(|row| (Measure::EndToEnd, row));
        let local = local.into_iter().map(|row| (Measure::Local, row));
        end_to_end.chain(local).collect()
    }

    /// Ends the recording of a collection that goes away and returns the rows of the open
    /// periods of both measures, which hold the arrivals up to the last observed frontier.
    pub fn finish(&mut self) -> Vec<(Measure, PeriodRow)> {
        let Some((end_to_end, local)) = self.recorders.as_mut() else {
            return Vec::new();
        };
        let end_to_end = end_to_end.finish().into_iter();
        let local = local.finish().into_iter();
        let end_to_end = end_to_end.map(|row| (Measure::EndToEnd, row));
        let local = local.map(|row| (Measure::Local, row));
        end_to_end.chain(local).collect()
    }
}

/// Records in `log` that the arrivals `frontier` reflects were visible at `now_ms`, and drops the
/// entries older than the retention of `config`.
pub fn record_visibility(
    log: &mut VisibilityLog,
    tree: &ResolvedTree,
    frontier: &Antichain<Timestamp>,
    now_ms: u64,
    config: &RecorderConfig,
) {
    let reflected = match frontier.as_option() {
        Some(t) => tree.last_reflected(*t),
        // A complete collection makes every arrival visible from now on. Without this entry its
        // consumers would find no answer for later arrivals and record a local delay of 0.
        None => Some(u64::MAX),
    };
    if let Some(reflected) = reflected {
        log.record(reflected, now_ms);
    }
    // Pruned by age, not through the least arrival its consumers recorded, because consumers in
    // other controllers read the log and tracking them would need a registration protocol. A
    // consumer that lags the input by more than the retention finds no entry and records those
    // arrivals with a local delay of 0, which understates and never overstates.
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
mod tests {
    use mz_repr::Timestamp;
    use mz_storage_types::freshness::histogram::BucketScheme;
    use mz_storage_types::freshness::recorder::RecorderConfig;
    use mz_storage_types::freshness::{ResolvedTree, Ticker};
    use timely::progress::Antichain;

    use super::SharedVisibilityLog;

    #[mz_ore::test]
    fn visibility_log_is_pruned_by_age() {
        let tree = ResolvedTree::Leaf(Ticker::Aligned { interval_ms: 1000 });
        let config = RecorderConfig {
            scheme: BucketScheme::new(4).expect("power of two"),
            interval_ms: 1,
            period_ms: 60_000,
        };
        let log = SharedVisibilityLog::default();
        assert_eq!(log.input_log(None).visible_at(1), None);
        // One advance per second for 15 minutes, observed 5 ms after each tick.
        for s in 1..=900u64 {
            let frontier = Antichain::from_elem(Timestamp::from(s * 1000 + 1));
            log.record(&tree, &frontier, s * 1000 + 5, &config);
        }
        let copy = log.input_log(None);
        // Ten minutes are kept.
        assert_eq!(copy.visible_at(300_000), Some(300_005));
        assert_eq!(copy.visible_at(299_000), None);
        assert_eq!(copy.visible_at(900_000), Some(900_005));
        // A copy for a consumer answers only what it has not recorded yet.
        let copy = log.input_log(Some(600_000));
        assert_eq!(copy.visible_at(600_000), None);
        assert_eq!(copy.visible_at(600_001), Some(601_005));
    }

    #[mz_ore::test]
    fn closed_collection_stays_visible_from_its_close() {
        let tree = ResolvedTree::Leaf(Ticker::Aligned { interval_ms: 1000 });
        let config = RecorderConfig {
            scheme: BucketScheme::new(4).expect("power of two"),
            interval_ms: 1,
            period_ms: 60_000,
        };
        let log = SharedVisibilityLog::default();
        log.record(
            &tree,
            &Antichain::from_elem(Timestamp::from(1001)),
            1005,
            &config,
        );
        log.record(&tree, &Antichain::new(), 2005, &config);
        // Later observations, which prune by age, keep the close.
        log.record(&tree, &Antichain::new(), 10_000_000, &config);
        let copy = log.input_log(Some(5_000_000));
        assert_eq!(copy.visible_at(5_000_001), Some(2005));
        assert_eq!(copy.visible_at(u64::MAX), Some(2005));
    }
}

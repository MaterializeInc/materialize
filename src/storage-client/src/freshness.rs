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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use chrono::{DateTime, Utc};
use mz_cluster_client::ReplicaId;
use mz_controller_types::dyncfgs::{
    ENABLE_WALLCLOCK_FRESHNESS_RECORDING, WALLCLOCK_FRESHNESS_BUCKETS_PER_OCTAVE,
    WALLCLOCK_FRESHNESS_PERIOD_INTERVAL,
};
use mz_dyncfg::ConfigSet;
use mz_ore::now::NowFn;
use mz_ore::soft_panic_or_log;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_storage_types::freshness::{
    BucketScheme, PeriodRow, RecorderConfig, StalenessRecorder, StalenessReporter, reflected_time,
};
use mz_storage_types::time_dependence::TimeDependence;
use timely::progress::Antichain;

use crate::controller::WallclockLag;

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
    /// The earliest reflected time, [`Timestamp::MIN`] unless the state was created by
    /// [`FreshnessState::new_created`].
    floor: Timestamp,
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
        Self::with_floor(dependence, start, Timestamp::MIN, frontier, config)
    }

    /// Creates the state of a collection created at `start`, as [`FreshnessState::new`], except
    /// that frontiers reflect no time before `start`.
    ///
    /// Its staleness at `now` is at most `now - start`, whatever frontier it starts with.
    pub fn new_created(
        dependence: TimeDependence,
        start: Timestamp,
        frontier: &Antichain<Timestamp>,
        config: Option<&RecorderConfig>,
    ) -> Self {
        Self::with_floor(dependence, start, start, frontier, config)
    }

    fn with_floor(
        dependence: TimeDependence,
        start: Timestamp,
        floor: Timestamp,
        frontier: &Antichain<Timestamp>,
        config: Option<&RecorderConfig>,
    ) -> Self {
        let reflected = reflect(&dependence, floor, frontier);
        let mut state = FreshnessState {
            dependence,
            start,
            floor,
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
        self.reflected = reflect(&self.dependence, self.floor, frontier);
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

    /// Restarts the state at `now` with the frontier `frontier`, after a time in which nobody
    /// observed its frontier, and returns the periods that closed.
    ///
    /// The time since the last observation is not charged, since its frontiers are unknown.
    pub fn resume(
        &mut self,
        now: Timestamp,
        frontier: &Antichain<Timestamp>,
        config: Option<&RecorderConfig>,
    ) -> Vec<PeriodRow> {
        if self.complete {
            return Vec::new();
        }
        // The open samples of the reporter and the recorder span the unobserved time.
        self.recorder = None;
        if frontier.is_empty() {
            return self.finish(now, config);
        }
        self.frontier.clone_from(frontier);
        self.reflected = reflect(&self.dependence, self.floor, frontier);
        self.reporter = StalenessReporter::new(now, self.reflected.unwrap_or(self.start));
        self.apply_config(now, config);
        Vec::new()
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

/// The staleness states of collections whose write frontiers only [`StorageCollections`] learns,
/// among them tables, webhooks, and storage-managed collections.
///
/// The background task of [`StorageCollections`] observes their advances when it learns them,
/// with [`UnreportedFreshness::observe`]. The storage controller creates, ticks, and finishes
/// the states under [`UnreportedFreshness::lock_synced`], and it alone writes the rows they close.
///
/// [`StorageCollections`]: crate::storage_collections::StorageCollections
#[derive(Debug, Default)]
pub struct UnreportedFreshness {
    /// Whether [`UnreportedFreshness::observe`] is called.
    ///
    /// The background task reads it while it holds the lock of its collections, and the
    /// controller reads those collections while it holds the lock of the states. Keeping it
    /// in the states would take the two locks in both orders.
    observing: AtomicBool,
    states: Mutex<UnreportedStates>,
}

impl UnreportedFreshness {
    /// Whether advances are to be observed.
    pub fn is_observing(&self) -> bool {
        self.observing.load(Ordering::SeqCst)
    }

    /// Locks the states, sets their recorder configuration to `config` and whether advances are
    /// observed to `observing`, and returns them with the time of `now` read under the lock.
    ///
    /// When observing turns on, every state resumes at that time from the frontier that
    /// `frontier_of` returns for it, see [`FreshnessState::resume`]. A state without one keeps
    /// its frontier. `frontier_of` may take the lock of the `StorageCollections` collections.
    pub fn lock_synced(
        &self,
        now: &NowFn,
        config: Option<RecorderConfig>,
        observing: bool,
        mut frontier_of: impl FnMut(GlobalId) -> Option<Antichain<Timestamp>>,
    ) -> (MutexGuard<'_, UnreportedStates>, Timestamp) {
        let mut states = self.lock();
        // Read under the lock, so that no observation is later than an operation on the states
        // at this time.
        let now = Timestamp::from(now());
        states.config = config;
        let was_observing = self.observing.swap(observing, Ordering::SeqCst);
        if observing && !was_observing {
            // Read after observing turned on and before unlocking. The background task observes
            // every advance these frontiers miss once it gets the lock, at a later time.
            for id in states.ids() {
                if let Some(frontier) = frontier_of(id) {
                    states.resume(id, now, &frontier);
                }
            }
        }
        (states, now)
    }

    fn lock(&self) -> MutexGuard<'_, UnreportedStates> {
        self.states.lock().expect("lock poisoned")
    }

    /// Observes the write frontiers that advanced to `advances`, at the time of `now`.
    ///
    /// Must not be called while holding the lock of the `StorageCollections` collections.
    pub fn observe(&self, now: &NowFn, advances: &[(GlobalId, Antichain<Timestamp>)]) {
        let mut guard = self.lock();
        // Read under the lock, so that no observation is earlier than the controller's last
        // operation on the states.
        let now = Timestamp::from(now());
        let UnreportedStates {
            config,
            states,
            rows,
        } = &mut *guard;
        for (id, frontier) in advances {
            if let Some(state) = states.get_mut(id) {
                let closed = state.observe(now, frontier, config.as_ref());
                if !closed.is_empty() {
                    rows.push((*id, closed));
                }
            }
        }
    }
}

/// The states of [`UnreportedFreshness`].
#[derive(Debug, Default)]
pub struct UnreportedStates {
    /// The recorder configuration of the states, `None` while recording is off.
    config: Option<RecorderConfig>,
    states: BTreeMap<GlobalId, FreshnessState>,
    /// The rows that closed and that the controller has not taken yet.
    rows: Vec<(GlobalId, Vec<PeriodRow>)>,
}

impl UnreportedStates {
    /// Adds the state of collection `id`.
    pub fn insert(&mut self, id: GlobalId, state: FreshnessState) {
        let previous = self.states.insert(id, state);
        mz_ore::soft_assert_or_log!(previous.is_none(), "staleness state of {id} replaced");
    }

    /// The state of collection `id`.
    pub fn get_mut(&mut self, id: &GlobalId) -> Option<&mut FreshnessState> {
        self.states.get_mut(id)
    }

    /// The collections that have a state.
    fn ids(&self) -> Vec<GlobalId> {
        self.states.keys().copied().collect()
    }

    /// Resumes the state of collection `id` at `now` with `frontier`, see
    /// [`FreshnessState::resume`].
    fn resume(&mut self, id: GlobalId, now: Timestamp, frontier: &Antichain<Timestamp>) {
        if let Some(state) = self.states.get_mut(&id) {
            let closed = state.resume(now, frontier, self.config.as_ref());
            self.push_rows(id, closed);
        }
    }

    /// Closes the periods of all states that ended at or before `now`.
    pub fn tick(&mut self, now: Timestamp) {
        for (id, state) in &mut self.states {
            let closed = state.tick(now, self.config.as_ref());
            if !closed.is_empty() {
                self.rows.push((*id, closed));
            }
        }
    }

    /// Removes the state of collection `id`, and ends its recording at `now`.
    pub fn finish(&mut self, id: GlobalId, now: Timestamp) {
        if let Some(mut state) = self.states.remove(&id) {
            let closed = state.finish(now, self.config.as_ref());
            self.push_rows(id, closed);
        }
    }

    /// Takes the rows that closed since the last call, by collection.
    pub fn take_rows(&mut self) -> Vec<(GlobalId, Vec<PeriodRow>)> {
        std::mem::take(&mut self.rows)
    }

    fn push_rows(&mut self, id: GlobalId, rows: Vec<PeriodRow>) {
        if !rows.is_empty() {
            self.rows.push((id, rows));
        }
    }
}

/// The wallclock lag to report at `now` for a readable frontier whose staleness `state` tracks.
///
/// `today` is the lag of the frontier as `WallclockLagFn` computes it, in whole seconds. With
/// `reporting` and a `state`, the lag is [`FreshnessState::reported_lag`] in milliseconds,
/// otherwise `today`. Callers pass each state once per refresh, also while the frontier is not
/// readable, since the read resets the state's peak.
pub fn reported_wallclock_lag(
    reporting: bool,
    state: Option<&mut FreshnessState>,
    now: Timestamp,
    today: Duration,
) -> WallclockLag {
    // Read also while reporting is off, so that turning it on reports no peak from the time it
    // was off.
    let reported = state.map(|state| state.reported_lag(now));
    match reported {
        Some(lag) if reporting => WallclockLag::from_duration(lag),
        _ => WallclockLag::from_duration(today),
    }
}

/// The reflected time of `frontier`, at least `floor`, `None` if it reflects no time.
fn reflect(
    dependence: &TimeDependence,
    floor: Timestamp,
    frontier: &Antichain<Timestamp>,
) -> Option<Timestamp> {
    frontier
        .as_option()
        .and_then(|frontier| reflected_time(dependence, *frontier))
        .map(|reflected| reflected.max(floor))
}

#[cfg(test)]
mod tests;

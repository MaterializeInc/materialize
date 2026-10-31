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
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use mz_cluster_client::ReplicaId;
use mz_controller_types::dyncfgs::{
    ENABLE_WALLCLOCK_FRESHNESS_RECORDING, WALLCLOCK_FRESHNESS_BUCKETS_PER_OCTAVE,
    WALLCLOCK_FRESHNESS_PERIOD_INTERVAL, WALLCLOCK_FRESHNESS_RESTART_POLICY,
    WALLCLOCK_FRESHNESS_VIRTUAL_ARRIVAL_INTERVAL,
};
use mz_dyncfg::ConfigSet;
use mz_ore::soft_panic_or_log;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{ColumnName, Datum, Diff, GlobalId, Row, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::dyncfgs::WALLCLOCK_FRESHNESS_HISTOGRAM_RETENTION_INTERVAL;
use mz_storage_types::freshness::histogram::BucketScheme;
use mz_storage_types::freshness::local::{
    LocalDelayRecorder, LocalInput, LocalRows, VisibilityLog,
};
use mz_storage_types::freshness::recorder::{FreshnessRecorder, PeriodRow, RecorderConfig};
use mz_storage_types::freshness::{FreshnessTree, ResolvedTree, TickerLookup};
use timely::progress::Antichain;

use crate::controller::WallclockLag;
use crate::healthcheck::{
    REPLICA_WALLCLOCK_FRESHNESS_HISTOGRAM_RAW_DESC, WALLCLOCK_FRESHNESS_HISTOGRAM_RAW_DESC,
};

/// What a freshness histogram row measures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
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

    fn from_column(s: &str) -> Option<Self> {
        match s {
            "end_to_end" => Some(Measure::EndToEnd),
            "local" => Some(Measure::Local),
            "skew" => Some(Measure::Skew),
            _ => None,
        }
    }
}

/// How the recordings of collections that existed before environmentd started begin.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestartPolicy {
    /// At the time the controller was created. The arrivals from the last emitted period up to
    /// that time are not recorded, which leaves a hole in the histogram.
    Hole,
    /// At the seed from [`FreshnessSeeds`], so the arrivals since the last emitted period count
    /// at the first observation, with the outage as latency.
    Charge,
}

impl RestartPolicy {
    /// The policy `wallclock_freshness_restart_policy` selects. Unknown values select `Hole`.
    pub fn get(dyncfg: &ConfigSet) -> Self {
        match WALLCLOCK_FRESHNESS_RESTART_POLICY.get(dyncfg).as_str() {
            "charge" => RestartPolicy::Charge,
            "hole" => RestartPolicy::Hole,
            other => {
                tracing::warn!("unknown wallclock freshness restart policy {other:?}, using hole");
                RestartPolicy::Hole
            }
        }
    }
}

/// The recording of the global write frontier of collection `id`, or of `replica_id`'s, that
/// existed before environmentd started, under the restart policy of `dyncfg`.
///
/// `boot_ms` is the wall-clock time at which the controller was created. All recordings after a
/// restart share it, so how long bootstrap takes does not move their start.
pub fn recording_after_restart(
    dyncfg: &ConfigSet,
    seeds: &FreshnessSeeds,
    id: GlobalId,
    replica_id: Option<ReplicaId>,
    boot_ms: u64,
) -> FreshnessRecording {
    match RestartPolicy::get(dyncfg) {
        RestartPolicy::Hole => FreshnessRecording::at_first_observation(boot_ms),
        RestartPolicy::Charge => FreshnessRecording::seeded(seeds.clone(), id, replica_id, boot_ms),
    }
}

/// The key of a restart seed: the object, the replica (`None` in the global histogram), and the
/// measure.
pub type SeedKey = (GlobalId, Option<ReplicaId>, Measure);

/// The restart seeds of freshness recordings: the maximum period end in the freshness histograms
/// per [`SeedKey`], as the histograms stood when environmentd started.
///
/// The write tasks of the two histogram relations publish them, and the controllers take them
/// when recordings start. Clones share the seeds.
#[derive(Clone, Default)]
pub struct FreshnessSeeds(Arc<Mutex<SeedState>>);

// NOTE: Summarizes instead of deriving, because every recording started with the seeds holds a
// clone, and controller state dumps would otherwise print all seeds once per recording.
impl fmt::Debug for FreshnessSeeds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("FreshnessSeeds");
        match self.0.try_lock() {
            Ok(state) => d
                .field("global_loaded", &state.global_loaded)
                .field("replica_loaded", &state.replica_loaded)
                .field("seeds", &state.seeds.len()),
            Err(_) => d.field("state", &"<unavailable>"),
        };
        d.finish()
    }
}

#[derive(Debug, Default)]
struct SeedState {
    global_loaded: bool,
    replica_loaded: bool,
    seeds: BTreeMap<SeedKey, u64>,
}

/// The seeds of the end-to-end and the local recorder of one recording.
#[derive(Debug)]
struct RecordingSeeds {
    end_to_end: Option<u64>,
    local: Option<u64>,
}

impl FreshnessSeeds {
    /// Publishes the seeds of the global histogram, or of the per-replica one if `per_replica`.
    pub fn publish(&self, per_replica: bool, seeds: BTreeMap<SeedKey, u64>) {
        let mut state = self.0.lock().expect("lock poisoned");
        match per_replica {
            false => state.global_loaded = true,
            true => state.replica_loaded = true,
        }
        state.seeds.extend(seeds);
    }

    /// Removes the seeds of the recording of `id` on `replica_id`, or globally, and returns them,
    /// `None` until the histogram they come from is published.
    fn take(&self, id: GlobalId, replica_id: Option<ReplicaId>) -> Option<RecordingSeeds> {
        let mut state = self.0.lock().expect("lock poisoned");
        let loaded = match replica_id {
            None => state.global_loaded,
            Some(_) => state.replica_loaded,
        };
        if !loaded {
            return None;
        }
        let mut take = |measure| state.seeds.remove(&(id, replica_id, measure));
        // Skew rows come from the local delay recorder, which the local seed starts.
        take(Measure::Skew);
        Some(RecordingSeeds {
            end_to_end: take(Measure::EndToEnd),
            local: take(Measure::Local),
        })
    }
}

/// Collects the restart seeds from the rows of a freshness histogram relation.
#[derive(Debug)]
pub struct SeedCollector {
    period_end_col: usize,
    object_id_col: usize,
    replica_id_col: Option<usize>,
    measure_col: usize,
    seeds: BTreeMap<SeedKey, u64>,
}

impl SeedCollector {
    /// Collects from the global histogram, or from the per-replica one if `per_replica`.
    pub fn new(per_replica: bool) -> Self {
        let desc = match per_replica {
            false => &*WALLCLOCK_FRESHNESS_HISTOGRAM_RAW_DESC,
            true => &*REPLICA_WALLCLOCK_FRESHNESS_HISTOGRAM_RAW_DESC,
        };
        let col = |name: &str| {
            desc.get_by_name(&ColumnName::from(name))
                .expect("schema has not changed")
                .0
        };
        SeedCollector {
            period_end_col: col("period_end"),
            object_id_col: col("object_id"),
            replica_id_col: per_replica.then(|| col("replica_id")),
            measure_col: col("measure"),
            seeds: BTreeMap::new(),
        }
    }

    /// Adds a row of the relation with multiplicity `diff`. Rows with a non-positive `diff` are
    /// not in the relation and do not seed.
    pub fn add(&mut self, datums: &[Datum], diff: StorageDiff) {
        if diff <= 0 {
            return;
        }
        let Some(key) = self.key(datums) else {
            soft_panic_or_log!("malformed freshness histogram row: {datums:?}");
            return;
        };
        let period_end = datums[self.period_end_col].unwrap_timestamptz();
        let Ok(period_end) = u64::try_from(period_end.timestamp_millis()) else {
            soft_panic_or_log!("freshness period end before the epoch: {period_end}");
            return;
        };
        let seed = self.seeds.entry(key).or_default();
        *seed = (*seed).max(period_end);
    }

    fn key(&self, datums: &[Datum]) -> Option<SeedKey> {
        let id = datums[self.object_id_col].unwrap_str().parse().ok()?;
        let replica_id = match self.replica_id_col {
            None => None,
            Some(col) => Some(datums[col].unwrap_str().parse().ok()?),
        };
        let measure = Measure::from_column(datums[self.measure_col].unwrap_str())?;
        Some((id, replica_id, measure))
    }

    /// The seeds of the added rows.
    pub fn finish(self) -> BTreeMap<SeedKey, u64> {
        self.seeds
    }
}

/// Where a recording starts, consulted until it does.
#[derive(Debug)]
enum Start {
    /// At the latest of what the as-of reflects, the wall-clock creation time in ms, and the
    /// retention before the first observation.
    Created(Antichain<Timestamp>, u64),
    /// At the wall-clock time in ms at which the recording was built, but no earlier than the
    /// retention before the first observation.
    FirstObservation(u64),
    /// At the seeds of the recording of `id` on `replica_id`, or globally, but no earlier than
    /// the retention before the first observation, see [`RestartPolicy::Charge`]. A measure
    /// without a seed starts like `FirstObservation(built_ms)`.
    Seeded {
        seeds: FreshnessSeeds,
        id: GlobalId,
        replica_id: Option<ReplicaId>,
        built_ms: u64,
    },
}

/// Freshness recording of one write frontier, the global one of a collection or a replica's.
#[derive(Debug)]
pub struct FreshnessRecording {
    start: Start,
    /// Whether the local delay and skew are recorded, see [`Self::end_to_end_only`].
    local: bool,
    /// The recorders of all recorded measures. They start together, and at the same arrival
    /// unless their restart seeds differ, so their per-period counts agree.
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

    /// Records the arrivals after `built_ms`, each once a frontier passes it, so the arrivals
    /// between `built_ms` and the first observed frontier count with their real latency. The
    /// arrivals at or before `built_ms` are not recorded, which leaves a hole in the histogram.
    pub fn at_first_observation(built_ms: u64) -> Self {
        FreshnessRecording {
            start: Start::FirstObservation(built_ms),
            local: true,
            recorders: None,
        }
    }

    /// Records the arrivals after the seed of each measure of `id` on `replica_id`, or globally,
    /// but none older than the retention, and a measure without a seed like
    /// [`Self::at_first_observation`] built at `built_ms`. Records nothing until the seeds of the
    /// histogram are published.
    ///
    /// The seeds are taken when the recording starts, so a later recording of the same frontier
    /// starts at its `built_ms`.
    pub fn seeded(
        seeds: FreshnessSeeds,
        id: GlobalId,
        replica_id: Option<ReplicaId>,
        built_ms: u64,
    ) -> Self {
        FreshnessRecording {
            start: Start::Seeded {
                seeds,
                id,
                replica_id,
                built_ms,
            },
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

    fn recorders(
        &self,
        end_to_end_start_ms: u64,
        local_start_ms: u64,
    ) -> (FreshnessRecorder, Option<LocalDelayRecorder>) {
        (
            FreshnessRecorder::new(end_to_end_start_ms),
            self.local.then(|| LocalDelayRecorder::new(local_start_ms)),
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
            let retained = now_ms.saturating_sub(config.retention_ms);
            let start = match &self.start {
                Start::Created(as_of, created_ms) => {
                    let reflected = reflected(as_of).unwrap_or(0);
                    let start = reflected.max(*created_ms).max(retained);
                    Some((start, start))
                }
                // A first frontier near the epoch, as of a collection whose as-of is `[0]`, records
                // nothing until it passes `built_ms`, so the time since the epoch is never charged
                // as latency.
                Start::FirstObservation(built_ms) => {
                    let start = (*built_ms).max(retained);
                    Some((start, start))
                }
                Start::Seeded {
                    seeds,
                    id,
                    replica_id,
                    built_ms,
                } => seeds.take(*id, *replica_id).map(|seeds| {
                    // Seeds are taken at the first observation, which can come long after they
                    // were published, for example on a cluster without replicas, so a seed can
                    // be older than the retention.
                    let start = |seed: Option<u64>| seed.unwrap_or(*built_ms).max(retained);
                    (start(seeds.end_to_end), start(seeds.local))
                }),
            };
            let Some((end_to_end, local)) = start else {
                return Vec::new();
            };
            self.recorders = Some(self.recorders(end_to_end, local));
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

/// A reader of a visibility log: the global recording of a collection, or its recording on a
/// replica.
pub type LogConsumer = (GlobalId, Option<ReplicaId>);

/// A collection's visibility log, and how far its consumers have recorded.
///
/// Entries are kept while a consumer may still need them, for as long as the consumer is
/// registered, which it is from its creation until it is dropped or its recording stops.
/// Entries older than one period are thinned geometrically by age, which bounds the log however
/// long a consumer lags, see [`VisibilityLog::thin`], and entries older than the histogram
/// retention are dropped.
#[derive(Debug, Default, Clone)]
pub struct ConsumedVisibilityLog {
    log: VisibilityLog,
    /// The last arrival each consumer recorded, `None` before its recording started.
    consumers: BTreeMap<LogConsumer, Option<u64>>,
}

impl ConsumedVisibilityLog {
    /// Records that `frontier`, and the arrivals it reflects, were visible at `now_ms`, and drops
    /// the entries no consumer needs anymore.
    pub fn record(
        &mut self,
        tree: &ResolvedTree,
        frontier: &Antichain<Timestamp>,
        now_ms: u64,
        config: &RecorderConfig,
    ) {
        let (frontier, reflected) = match frontier.as_option() {
            Some(t) => (u64::from(*t), tree.last_reflected(*t)),
            // A complete collection makes every arrival visible from now on. Without this entry
            // its consumers would find no answer for later arrivals and record a local delay of 0.
            None => (u64::MAX, Some(u64::MAX)),
        };
        self.log.record(frontier, reflected, now_ms);
        // `None` sorts first, so a consumer whose recording has not started keeps everything,
        // subject to thinning.
        match self.consumers.values().min() {
            Some(Some(through)) => self.log.prune_through_except_last(*through),
            Some(None) => {}
            // The last entry stays for consumers that register later.
            None => self.log.prune_through_except_last(u64::MAX),
        }
        self.log.thin(now_ms, config.period_ms, &config.scheme);
        // Recorders skip arrivals older than the retention, so no consumer reads these.
        self.log
            .prune_observed_before(now_ms.saturating_sub(config.retention_ms));
    }

    /// Registers `consumer`, so that the log keeps the entries it needs before its first read.
    /// Does nothing if it is registered already.
    pub fn register(&mut self, consumer: LogConsumer) {
        self.consumers.entry(consumer).or_insert(None);
    }

    /// Forgets `consumer`.
    pub fn deregister(&mut self, consumer: LogConsumer) {
        self.consumers.remove(&consumer);
    }

    /// Notes that `consumer` has recorded through `recorded_through`, if it is registered.
    pub fn mark(&mut self, consumer: LogConsumer, recorded_through: Option<u64>) {
        if let Some(mark) = self.consumers.get_mut(&consumer) {
            *mark = recorded_through;
        }
    }

    /// A copy that answers the arrivals above `recorded_through` for `consumer`, which has
    /// recorded through `recorded_through`. Registers `consumer` if needed.
    pub fn read(&mut self, consumer: LogConsumer, recorded_through: Option<u64>) -> VisibilityLog {
        self.consumers.insert(consumer, recorded_through);
        self.log.after(recorded_through.unwrap_or(0))
    }

    /// The number of log entries.
    pub fn len(&self) -> usize {
        self.log.len()
    }

    /// Whether the log has no entries.
    pub fn is_empty(&self) -> bool {
        self.log.is_empty()
    }
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

/// Caches the resolution of a collection's freshness tree.
///
/// Resolving looks up the ticker of every ingestion leaf, which can take a lock that other
/// components contend on, so it only runs when the tree or the lookup's generation changed.
#[derive(Debug, Default)]
pub struct ResolvedTreeCache {
    cached: Option<CachedResolution>,
}

#[derive(Debug)]
struct CachedResolution {
    generation: u64,
    tree: Option<FreshnessTree>,
    resolved: Option<Arc<ResolvedTree>>,
}

impl ResolvedTreeCache {
    /// [`FreshnessTree::resolve`] of `tree`.
    pub fn resolve(
        &mut self,
        tree: &FreshnessTree,
        lookup: &dyn TickerLookup,
    ) -> Option<Arc<ResolvedTree>> {
        // Read before resolving, so that a ticker changing during resolution leaves a stale
        // generation behind and the next call resolves again.
        let generation = lookup.generation();
        match &self.cached {
            Some(c) if c.generation == generation && c.tree.as_ref() == Some(tree) => {
                c.resolved.clone()
            }
            _ => self.store(generation, Some(tree.clone()), lookup),
        }
    }

    /// Like [`Self::resolve`] for a tree that `fetch` returns, which is only called when the
    /// lookup's generation changed. Callers must ensure that every change of the fetched tree
    /// also changes the generation.
    pub fn resolve_with(
        &mut self,
        lookup: &dyn TickerLookup,
        fetch: impl FnOnce() -> Option<FreshnessTree>,
    ) -> Option<Arc<ResolvedTree>> {
        let generation = lookup.generation();
        match &self.cached {
            Some(c) if c.generation == generation => c.resolved.clone(),
            _ => self.store(generation, fetch(), lookup),
        }
    }

    fn store(
        &mut self,
        generation: u64,
        tree: Option<FreshnessTree>,
        lookup: &dyn TickerLookup,
    ) -> Option<Arc<ResolvedTree>> {
        let resolved = tree.as_ref().and_then(|t| t.resolve(lookup)).map(Arc::new);
        self.cached = Some(CachedResolution {
            generation,
            tree,
            resolved: resolved.clone(),
        });
        resolved
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
pub struct SharedVisibilityLog(Arc<Mutex<ConsumedVisibilityLog>>);

impl SharedVisibilityLog {
    /// Calls `f` with exclusive access to the log.
    pub fn with<R>(&self, f: impl FnOnce(&mut ConsumedVisibilityLog) -> R) -> R {
        f(&mut self.0.lock().expect("lock poisoned"))
    }

    /// Like [`ConsumedVisibilityLog::record`].
    pub fn record(
        &self,
        tree: &ResolvedTree,
        frontier: &Antichain<Timestamp>,
        now_ms: u64,
        config: &RecorderConfig,
    ) {
        self.with(|log| log.record(tree, frontier, now_ms, config));
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

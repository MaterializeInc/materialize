// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Worker-local state for compute timely instances.

use std::any::Any;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytesize::ByteSize;
use differential_dataflow::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::TraceReader;
use mz_compute_client::logging::LoggingConfig;
use mz_compute_client::protocol::command::{
    ComputeCommand, ComputeParameters, InstanceConfig, Peek, PeekTarget,
};
use mz_compute_client::protocol::history::ComputeCommandHistory;
use mz_compute_client::protocol::response::{
    ComputeResponse, CopyToResponse, FrontiersResponse, PeekError, PeekResponse, SubscribeResponse,
};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::dyncfgs::{
    ENABLE_PEEK_RESPONSE_STASH, ENABLE_PEEK_ROW_ITERATION_LIMIT,
    PEEK_RESPONSE_STASH_BATCH_MAX_RUNS, PEEK_RESPONSE_STASH_THRESHOLD_BYTES,
    PEEK_ROW_ITERATION_LIMIT, PEEK_STASH_BATCH_SIZE, PEEK_STASH_NUM_BATCHES,
};
use mz_compute_types::plan::render_plan::RenderPlan;
use mz_dyncfg::{ConfigSet, ConfigValHandle};
use mz_expr::SafeMfpPlan;
use mz_expr::row::RowCollection;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::{MetricsRegistry, UIntGauge};
use mz_ore::now::EpochMillis;
use mz_ore::soft_panic_or_log;
use mz_ore::task::AbortOnDropHandle;
use mz_ore::tracing::{OpenTelemetryContext, TracingHandle};
use mz_persist_client::Diagnostics;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::USE_CRITICAL_SINCE_SNAPSHOT;
use mz_persist_client::read::ReadHandle;
use mz_persist_types::PersistLocation;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{DatumVec, GlobalId, Row, RowArena, Timestamp};
use mz_storage_operators::stats::StatsCursor;
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::dyncfgs::ORE_OVERFLOWING_BEHAVIOR;
use mz_storage_types::sources::SourceData;
use mz_storage_types::time_dependence::TimeDependence;
use mz_txn_wal::operator::TxnsContext;
use mz_txn_wal::txn_cache::TxnsCache;
use timely::dataflow::operators::probe;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::{oneshot, watch};
use tracing::{Level, debug, error, info, span, trace, warn};
use uuid::Uuid;

use crate::arrangement::manager::{TraceBundle, TraceManager};
use crate::logging;
use crate::logging::compute::{CollectionLogging, ComputeEvent, PeekEvent};
use crate::logging::initialize::LoggingTraces;
use crate::metrics::{CollectionMetrics, WorkerMetrics};
use crate::render::{LinearJoinSpec, StartSignal};
use crate::server::{ComputeInstanceContext, ResponseSender};

mod error_scan;
mod peek_budget;
mod peek_metrics;
mod peek_offload;
mod peek_result_iterator;
mod peek_scan;
mod peek_stash;

use self::peek_budget::InlineBudget;
use self::peek_metrics::IndexPeekMetrics;
use self::peek_metrics::PeekWalkMetrics;
pub(crate) use self::peek_offload::PeekPermits;
use self::peek_offload::{OffloadConfig, OffloadOutcome, OffloadedPeek};
use self::peek_scan::{IndexPeekScan, PeekScan, ScanOutcome};

/// Cheap handles on the dyncfgs that bound how many rows a peek may examine.
///
/// The limit is read through handles rather than captured once, because `UpdateConfiguration`
/// applies to peeks that are already in flight.
#[derive(Clone, Debug)]
struct PeekRowIterationConfig {
    enabled: ConfigValHandle<bool>,
    limit: ConfigValHandle<usize>,
}

impl PeekRowIterationConfig {
    fn new(config: &ConfigSet) -> Self {
        Self {
            enabled: ENABLE_PEEK_ROW_ITERATION_LIMIT.handle(config),
            limit: PEEK_ROW_ITERATION_LIMIT.handle(config),
        }
    }

    fn current_limit(&self) -> Option<usize> {
        self.enabled.get().then(|| self.limit.get())
    }
}

/// Counts the rows a peek has examined on this worker and fails it once that exceeds the limit.
///
/// A "row" here is a record the worker had to look at, not a record it returned. Records that a
/// literal constraint or the MFP throws away, and records that consolidate to zero, cost scan
/// time all the same, so they count too.
///
/// Exactly `limit` rows are allowed. The peek only fails when it asks for the row after that.
#[derive(Debug)]
pub(crate) struct PeekRowIterationTracker {
    limit: Option<usize>,
    rows_iterated: usize,
}

impl PeekRowIterationTracker {
    fn new(limit: Option<usize>, rows_iterated: usize) -> Self {
        Self {
            limit,
            rows_iterated,
        }
    }

    /// Adopts a new limit without forgetting the rows already examined.
    ///
    /// Rows counted while the feature was off still count, so turning it on mid-scan accounts for
    /// the work the peek has already caused.
    fn set_limit(&mut self, limit: Option<usize>) {
        self.limit = limit;
    }

    fn rows_iterated(&self) -> usize {
        self.rows_iterated
    }

    /// Adds rows examined by a walk that ran before this one.
    ///
    /// The limit bounds a peek rather than a single walk, so a walk that continues another one
    /// starts from the count that one reached.
    fn add_rows_iterated(&mut self, rows_iterated: usize) {
        self.rows_iterated = self.rows_iterated.saturating_add(rows_iterated);
    }

    fn track_next(&mut self) -> Result<(), PeekError> {
        if let Some(limit) = self.limit
            && self.rows_iterated >= limit
        {
            return Err(PeekError::RowIterationLimitExceeded { limit });
        }

        self.rows_iterated = self.rows_iterated.saturating_add(1);
        Ok(())
    }
}

fn peek_row_iteration_limit(config: &ConfigSet) -> Option<usize> {
    ENABLE_PEEK_ROW_ITERATION_LIMIT
        .get(config)
        .then(|| PEEK_ROW_ITERATION_LIMIT.get(config))
}

#[cfg(test)]
mod tests {
    use mz_dyncfg::ConfigUpdates;

    use super::*;

    #[mz_ore::test]
    fn row_iteration_limit_observes_updates_and_disabled_rows() {
        let config = mz_dyncfgs::all_dyncfgs();
        let row_iteration_config = PeekRowIterationConfig::new(&config);
        let mut tracker = PeekRowIterationTracker::new(row_iteration_config.current_limit(), 0);

        tracker.track_next().unwrap();
        tracker.track_next().unwrap();

        let mut updates = ConfigUpdates::default();
        updates.add(&PEEK_ROW_ITERATION_LIMIT, 3);
        updates.add(&ENABLE_PEEK_ROW_ITERATION_LIMIT, true);
        updates.apply(&config);
        tracker.set_limit(row_iteration_config.current_limit());
        tracker.track_next().unwrap();

        let mut updates = ConfigUpdates::default();
        updates.add(&ENABLE_PEEK_ROW_ITERATION_LIMIT, false);
        updates.apply(&config);
        tracker.set_limit(row_iteration_config.current_limit());
        tracker.track_next().unwrap();

        let mut updates = ConfigUpdates::default();
        updates.add(&PEEK_ROW_ITERATION_LIMIT, 5);
        updates.add(&ENABLE_PEEK_ROW_ITERATION_LIMIT, true);
        updates.apply(&config);
        tracker.set_limit(row_iteration_config.current_limit());
        tracker.track_next().unwrap();
        assert_eq!(
            tracker.track_next(),
            Err(PeekError::RowIterationLimitExceeded { limit: 5 })
        );
    }
}

/// Worker-local state that is maintained across dataflows.
///
/// This state is restricted to the COMPUTE state, the deterministic, idempotent work
/// done between data ingress and egress.
pub struct ComputeState {
    /// State kept for each installed compute collection.
    ///
    /// Each collection has exactly one frontier.
    /// How the frontier is communicated depends on the collection type:
    ///  * Frontiers of indexes are equal to the frontier of their corresponding traces in the
    ///    `TraceManager`.
    ///  * Persist sinks store their current frontier in `CollectionState::sink_write_frontier`.
    ///  * Subscribes report their frontiers through the `subscribe_response_buffer`.
    pub collections: BTreeMap<GlobalId, CollectionState>,
    /// The traces available for sharing across dataflows.
    pub traces: TraceManager,
    /// Shared buffer with SUBSCRIBE operator instances by which they can respond.
    ///
    /// The entries are pairs of sink identifier (to identify the subscribe instance)
    /// and the response itself.
    pub subscribe_response_buffer: Rc<RefCell<Vec<(GlobalId, SubscribeResponse)>>>,
    /// Shared buffer with S3 oneshot operator instances by which they can respond.
    ///
    /// The entries are pairs of sink identifier (to identify the s3 oneshot instance)
    /// and the response itself.
    pub copy_to_response_buffer: Rc<RefCell<Vec<(GlobalId, CopyToResponse)>>>,
    /// Peek commands that are awaiting fulfillment.
    pub pending_peeks: BTreeMap<Uuid, PendingPeek>,
    /// The persist location where we can stash large peek results.
    pub peek_stash_persist_location: Option<PersistLocation>,
    /// The logger, from Timely's logging framework, if logs are enabled.
    pub compute_logger: Option<logging::compute::Logger>,
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    /// This is intentionally shared between workers.
    pub persist_clients: Arc<PersistClientCache>,
    /// Context necessary for rendering txn-wal operators.
    pub txns_ctx: TxnsContext,
    /// History of commands received by this workers and all its peers.
    pub command_history: ComputeCommandHistory<UIntGauge>,
    /// Max size in bytes of any result.
    max_result_size: u64,
    /// Specification for rendering linear joins.
    pub linear_join_spec: LinearJoinSpec,
    /// Metrics for this worker.
    pub metrics: WorkerMetrics,
    /// A process-global handle to tracing configuration.
    tracing_handle: Arc<TracingHandle>,
    /// Other configuration for compute
    pub context: ComputeInstanceContext,
    /// Per-worker dynamic configuration.
    ///
    /// This is separate from the process-global `ConfigSet` and contains config options that need
    /// to be applied consistently with compute command order.
    ///
    /// For example, for options that influence dataflow rendering it is important that all workers
    /// render the same dataflow with the same options. If these options were stored in a global
    /// `ConfigSet`, we couldn't guarantee that all workers observe changes to them at the same
    /// point in the stream of compute commands. Storing per-worker configuration ensures that
    /// because each worker's configuration is only updated once that worker observes the
    /// respective `UpdateConfiguration` command.
    ///
    /// Reference-counted to avoid cloning for `Context`.
    pub worker_config: Rc<ConfigSet>,

    /// The process-global metrics registry.
    pub metrics_registry: MetricsRegistry,

    /// The number of timely workers per process.
    pub workers_per_process: usize,

    /// Bounds how many promoted peek walks run at once, shared with every other worker in this
    /// process.
    pub peek_permits: Arc<PeekPermits>,

    /// The metrics an index peek walk reports, whichever driver runs it.
    ///
    /// Held here rather than assembled per peek, because a promotion clones it into the task and
    /// the inline driver reads it on every activation of every pending peek.
    peek_walk_metrics: PeekWalkMetrics,

    /// What this activation may spend walking index peeks on the worker, and what is left of it.
    ///
    /// The activation is begun at the top of every sweep and armed from the parameters by the
    /// first peek that asks for a slice, which is not always a peek the sweep visits: a peek
    /// arriving between two sweeps is granted its first slice where it arrives. Such a peek draws
    /// from what the last sweep left, so a batch of arriving peeks costs at most one more
    /// aggregate rather than one inline budget per peek in the batch.
    peek_budget: InlineBudget,

    /// The pending peek a sweep stopped at for want of budget, and where the next sweep starts.
    ///
    /// Serving the pending peeks in uuid order alone would let a peek that arrives with a lower
    /// uuid take the turn of one an earlier sweep passed over. Resuming here makes the sweep a
    /// ring, so a peek that was passed over is served before the peeks that were served ahead of
    /// it. A stale entry is harmless: a uuid that has since been answered still names where in the
    /// ordering to resume.
    peek_resume_at: Option<Uuid>,

    /// Collections awaiting schedule instruction by the controller.
    ///
    /// Each entry stores a reference to a token that can be dropped to unsuspend the collection's
    /// dataflow. Multiple collections can reference the same token if they are exported by the
    /// same dataflow.
    suspended_collections: BTreeMap<GlobalId, Rc<dyn Any>>,

    /// Interval at which to perform server maintenance tasks. Set to a zero interval to
    /// perform maintenance with every `step_or_park` invocation.
    pub server_maintenance_interval: Duration,

    /// The [`mz_ore::now::SYSTEM_TIME`] at which the replica was started.
    ///
    /// Used to compute `replica_expiration`.
    pub init_system_time: EpochMillis,

    /// The maximum time for which the replica is expected to live. If not empty, dataflows in the
    /// replica can drop diffs associated with timestamps beyond the replica expiration.
    /// The replica will panic if such dataflows are not dropped before the replica has expired.
    pub replica_expiration: Antichain<Timestamp>,

    /// The storage worker forwards its introspection logs to the compute worker.
    pub storage_log_reader: Option<crate::server::StorageTimelyLogReader>,
}

impl ComputeState {
    /// Construct a new `ComputeState`.
    pub fn new(
        persist_clients: Arc<PersistClientCache>,
        txns_ctx: TxnsContext,
        metrics: WorkerMetrics,
        tracing_handle: Arc<TracingHandle>,
        context: ComputeInstanceContext,
        metrics_registry: MetricsRegistry,
        workers_per_process: usize,
        peek_permits: Arc<PeekPermits>,
        storage_log_reader: Option<crate::server::StorageTimelyLogReader>,
    ) -> Self {
        let worker_config: Rc<ConfigSet> = mz_dyncfgs::all_dyncfgs().into();
        let traces = TraceManager::new(metrics.clone());
        let command_history = ComputeCommandHistory::new(metrics.for_history());
        let peek_walk_metrics = PeekWalkMetrics::new(&metrics);
        let peek_budget = InlineBudget::new(&worker_config);

        Self {
            collections: Default::default(),
            traces,
            subscribe_response_buffer: Default::default(),
            copy_to_response_buffer: Default::default(),
            pending_peeks: Default::default(),
            peek_stash_persist_location: None,
            compute_logger: None,
            persist_clients,
            txns_ctx,
            command_history,
            max_result_size: u64::MAX,
            linear_join_spec: Default::default(),
            metrics,
            tracing_handle,
            context,
            worker_config,
            metrics_registry,
            workers_per_process,
            peek_permits,
            peek_walk_metrics,
            peek_budget,
            peek_resume_at: None,
            suspended_collections: Default::default(),
            server_maintenance_interval: Duration::ZERO,
            init_system_time: mz_ore::now::SYSTEM_TIME(),
            replica_expiration: Antichain::default(),
            storage_log_reader,
        }
    }

    /// Return a mutable reference to the identified collection.
    ///
    /// Panics if the collection doesn't exist.
    pub fn expect_collection_mut(&mut self, id: GlobalId) -> &mut CollectionState {
        self.collections
            .get_mut(&id)
            .expect("collection must exist")
    }

    /// Construct a new frontier probe for the given input and add it to the state of the given
    /// collections.
    ///
    /// The caller is responsible for attaching the returned probe handle to the respective
    /// dataflow input stream.
    pub fn input_probe_for(
        &mut self,
        input_id: GlobalId,
        collection_ids: impl Iterator<Item = GlobalId>,
    ) -> probe::Handle<Timestamp> {
        let probe = probe::Handle::default();
        for id in collection_ids {
            if let Some(collection) = self.collections.get_mut(&id) {
                collection.input_probes.insert(input_id, probe.clone());
            }
        }
        probe
    }

    /// Apply the current `worker_config` to the compute state.
    fn apply_worker_config(&mut self) {
        use mz_compute_types::dyncfgs::*;

        let config = &self.worker_config;

        self.linear_join_spec = LinearJoinSpec::from_config(config);

        if ENABLE_LGALLOC.get(config) {
            if let Some(path) = &self.context.scratch_directory {
                let clear_bytes = LGALLOC_SLOW_CLEAR_BYTES.get(config);
                let eager_return = ENABLE_LGALLOC_EAGER_RECLAMATION.get(config);
                let file_growth_dampener = LGALLOC_FILE_GROWTH_DAMPENER.get(config);
                let interval = LGALLOC_BACKGROUND_INTERVAL.get(config);
                let local_buffer_bytes = LGALLOC_LOCAL_BUFFER_BYTES.get(config);
                info!(
                    ?path,
                    backgrund_interval=?interval,
                    clear_bytes,
                    eager_return,
                    file_growth_dampener,
                    local_buffer_bytes,
                    "enabling lgalloc"
                );
                let background_worker_config = lgalloc::BackgroundWorkerConfig {
                    interval,
                    clear_bytes,
                };
                lgalloc::lgalloc_set_config(
                    lgalloc::LgAlloc::new()
                        .enable()
                        .with_path(path.clone())
                        .with_background_config(background_worker_config)
                        .eager_return(eager_return)
                        .file_growth_dampener(file_growth_dampener)
                        .local_buffer_bytes(local_buffer_bytes),
                );
            } else {
                debug!("not enabling lgalloc, scratch directory not specified");
            }
        } else {
            info!("disabling lgalloc");
            lgalloc::lgalloc_set_config(lgalloc::LgAlloc::new().disable());
        }

        // Pager backend selection follows scratch-directory availability:
        // a scratch dir means the file backend; no scratch dir means swap.
        // `set_scratch_dir` and `set_backend` are both idempotent, so calling
        // on every `apply_worker_config` tick is safe. The pager module is
        // only compiled on Unix targets (`mz_ore::pager` is `cfg(unix)`).
        #[cfg(unix)]
        if let Some(path) = &self.context.scratch_directory {
            mz_ore::pager::set_scratch_dir(path.clone());
            mz_ore::pager::set_backend(mz_ore::pager::Backend::File);
        } else {
            mz_ore::pager::set_backend(mz_ore::pager::Backend::Swap);
        }

        crate::memory_limiter::apply_limiter_config(config);

        mz_ore::region::ENABLE_LGALLOC_REGION.store(
            ENABLE_COLUMNATION_LGALLOC.get(config),
            std::sync::atomic::Ordering::Relaxed,
        );

        // NB: arrangement dictionary compression is deliberately NOT applied here. Unlike the
        // settings above, it is captured once at replica creation (see `handle_create_instance`
        // and `InstanceConfig::arrangement_dictionary_compression`) and held fixed, so that
        // flipping the flag does not retroactively change arrangements on existing replicas.

        // Apply column-paged-batcher configuration. Routes through
        // `apply_tiered_config`, which reuses a process-wide `TieredPolicy`
        // singleton — operator-driven tunes mutate the existing atomics
        // rather than installing a fresh policy with a fresh budget atomic
        // that would orphan in-flight resident tickets.
        //
        // Backend selection mirrors the lower-level `mz_ore::pager`
        // already configured above: file when a scratch directory is
        // available, swap otherwise.
        {
            use mz_ore::pager::Backend;
            use mz_timely_util::column_pager::{Codec, apply_tiered_config};

            let enabled = ENABLE_COLUMN_PAGED_BATCHER_SPILL.get(config);
            let codec = COLUMN_PAGED_BATCHER_LZ4.get(config).then_some(Codec::Lz4);
            let swap_pageout = COLUMN_PAGED_BATCHER_SWAP_PAGEOUT.get(config);

            // Budget derivation: fraction × announced memory limit, with a
            // 128 MiB floor so the no-pressure case doesn't page per chunk.
            // Falls back to a 4 GiB assumption if no limit was announced
            // (e.g. dev environments).
            const MIB: usize = 1024 * 1024;
            const DEFAULT_MEM_LIMIT: usize = 4 * 1024 * MIB;
            let mem_limit = crate::memory_limiter::get_memory_limit().unwrap_or(DEFAULT_MEM_LIMIT);
            let fraction = COLUMN_PAGED_BATCHER_BUDGET_FRACTION.get(config).max(0.0);
            let total = usize::cast_lossy(f64::cast_lossy(mem_limit) * fraction).max(128 * MIB);

            let backend = if self.context.scratch_directory.is_some() {
                Backend::File
            } else {
                Backend::Swap
            };

            debug!(
                enabled,
                ?backend,
                ?codec,
                swap_pageout,
                fraction,
                mem_limit,
                budget_bytes = total,
                "column-paged batcher: applying tiered config",
            );
            apply_tiered_config(enabled, total, backend, codec, swap_pageout);
        }

        // Install and retune the process-wide buffer pool that backs chunk
        // spilling. Installation is the gate. The pool is constructed, and its
        // MAP_NORESERVE address space reserved and spill threads spawned, only
        // when a config apply runs with a spill gate on, so a process that
        // never enables spilling never mmaps the pool. Config application
        // reruns on every UpdateConfiguration, so flipping a gate on installs
        // the pool on the next tick. The pool is a process singleton with no
        // teardown: once installed it stays active for the life of the process.
        // Turning both gates back off makes this block do nothing, so the pool
        // keeps its last-applied budget rather than being uninstalled. Later
        // ticks with a gate on retune the one instance in place.
        //
        // Storage's stash shares the singleton and gates only participation,
        // so its spill gate installs the pool too. The worker config set is
        // the full dyncfg aggregate, which is what makes the storage flag
        // readable here.
        {
            use mz_timely_util::pool_config::{PoolPagerConfig, apply_pool_config};

            let compute_spill = ENABLE_COLUMN_PAGED_BATCHER_SPILL.get(config);
            let storage_spill = mz_storage_types::dyncfgs::ENABLE_UPSERT_PAGED_SPILL.get(config);
            if !(compute_spill || storage_spill) {
                debug!("chunk spill: gates off, leaving the buffer pool uninstalled");
            } else {
                let spill_threads = COLUMN_PAGED_BATCHER_SPILL_WORKER_COUNT.get(config);
                let eager_backing = COLUMN_PAGED_BATCHER_EAGER_BACKING.get(config);

                // Budget derivation: fraction of physical RAM, with a 128 MiB
                // floor so the no-pressure case doesn't page per chunk.
                // Resident budgets derive from RAM, never from the announced
                // memory limit, which on swap-provisioned nodes deliberately
                // includes swap for the memory limiter's purposes. Falls back
                // to a 4 GiB assumption if detection fails.
                const MIB: usize = 1024 * 1024;
                const DEFAULT_RAM: usize = 4 * 1024 * MIB;
                let ram = mz_ore::memory::physical_memory_bytes().unwrap_or(DEFAULT_RAM);
                let of_ram =
                    |fraction: f64| usize::cast_lossy(f64::cast_lossy(ram) * fraction.max(0.0));
                let fraction = COLUMN_PAGED_BATCHER_BUDGET_FRACTION.get(config);
                let total = of_ram(fraction).max(128 * MIB);
                // No ordering is enforced between the target and the budget. A
                // target at or below budget + warm cap leaves no compressed-tier
                // headroom, which legally collapses the tier. Every backing
                // write then pages out immediately, the pre-tier behavior.
                let rss_target = of_ram(COLUMN_PAGED_BATCHER_POOL_RSS_TARGET_FRACTION.get(config));

                let applied = apply_pool_config(PoolPagerConfig {
                    budget_bytes: total,
                    spill_threads,
                    eager_backing,
                    rss_target_bytes: rss_target,
                });
                if applied {
                    info!(
                        compute_spill,
                        storage_spill,
                        fraction,
                        ram,
                        budget_bytes = total,
                        spill_threads,
                        eager_backing,
                        rss_target_bytes = rss_target,
                        "chunk spill: applying buffer-pool config",
                    );
                } else {
                    warn!("chunk spill: buffer pool unavailable; chunks stay resident");
                }
            }

            // The generational depth floor below which spilled bodies store
            // uncompressed. Subsystem-independent, so applied here alongside
            // the rest of the process-wide chunk configuration.
            let compress_min_depth =
                u8::try_from(COLUMN_CHUNK_COMPRESS_MIN_DEPTH.get(config)).unwrap_or(u8::MAX);
            mz_timely_util::columnar::chunk::set_compress_min_depth(compress_min_depth);
        }

        // Remember the maintenance interval locally to avoid reading it from the config set on
        // every server iteration.
        self.server_maintenance_interval = COMPUTE_SERVER_MAINTENANCE_INTERVAL.get(config);

        let overflowing_behavior = ORE_OVERFLOWING_BEHAVIOR.get(config);
        match overflowing_behavior.parse() {
            Ok(behavior) => mz_ore::overflowing::set_behavior(behavior),
            Err(err) => {
                error!(
                    err,
                    overflowing_behavior, "Invalid value for ore_overflowing_behavior"
                );
            }
        }
    }

    /// Apply the provided replica expiration `offset` by converting it to a frontier relative to
    /// the replica's initialization system time.
    ///
    /// Only expected to be called once when creating the instance. Guards against calling it
    /// multiple times by checking if the local expiration time is set.
    pub fn apply_expiration_offset(&mut self, offset: Duration) {
        if self.replica_expiration.is_empty() {
            let offset: EpochMillis = offset
                .as_millis()
                .try_into()
                .expect("duration must fit within u64");
            let replica_expiration_millis = self.init_system_time + offset;
            let replica_expiration = Timestamp::from(replica_expiration_millis);

            info!(
                offset = %offset,
                replica_expiration_millis = %replica_expiration_millis,
                replica_expiration_utc = %mz_ore::now::to_datetime(replica_expiration_millis),
                "setting replica expiration",
            );
            self.replica_expiration = Antichain::from_elem(replica_expiration);

            // Record the replica expiration in the metrics.
            self.metrics
                .replica_expiration_timestamp_seconds
                .set(replica_expiration.into());
        }
    }

    /// Returns the cc or non-cc version of "dataflow_max_inflight_bytes", as
    /// appropriate to this replica.
    pub fn dataflow_max_inflight_bytes(&self) -> Option<usize> {
        use mz_compute_types::dyncfgs::{
            DATAFLOW_MAX_INFLIGHT_BYTES, DATAFLOW_MAX_INFLIGHT_BYTES_CC,
        };

        if self.persist_clients.cfg.is_cc_active {
            DATAFLOW_MAX_INFLIGHT_BYTES_CC.get(&self.worker_config)
        } else {
            DATAFLOW_MAX_INFLIGHT_BYTES.get(&self.worker_config)
        }
    }
}

/// A wrapper around [ComputeState] with a live timely worker and response channel.
pub(crate) struct ActiveComputeState<'a> {
    /// The underlying Timely worker.
    pub timely_worker: &'a mut TimelyWorker,
    /// The compute state itself.
    pub compute_state: &'a mut ComputeState,
    /// The channel over which frontier information is reported.
    pub response_tx: &'a mut ResponseSender,
}

/// A token that keeps a sink alive.
pub struct SinkToken(#[allow(dead_code)] Box<dyn Any>);

impl SinkToken {
    /// Create a new `SinkToken`.
    pub fn new(t: Box<dyn Any>) -> Self {
        Self(t)
    }
}

impl<'a> ActiveComputeState<'a> {
    /// Entrypoint for applying a compute command.
    #[mz_ore::instrument(level = "debug")]
    pub fn handle_compute_command(&mut self, cmd: ComputeCommand) {
        use ComputeCommand::*;

        self.compute_state.command_history.push(cmd.clone());

        // Record the command duration, per worker and command kind.
        let timer = self
            .compute_state
            .metrics
            .handle_command_duration_seconds
            .for_command(&cmd)
            .start_timer();

        match cmd {
            Hello { .. } => panic!("Hello must be captured before"),
            CreateInstance(instance_config) => self.handle_create_instance(*instance_config),
            InitializationComplete => (),
            UpdateConfiguration(params) => self.handle_update_configuration(*params),
            CreateDataflow(dataflow) => self.handle_create_dataflow(*dataflow),
            Schedule(id) => self.handle_schedule(id),
            AllowCompaction { id, frontier } => self.handle_allow_compaction(id, frontier),
            Peek(peek) => {
                peek.otel_ctx.attach_as_parent();
                self.handle_peek(*peek)
            }
            CancelPeek { uuid } => self.handle_cancel_peek(uuid),
            AllowWrites(id) => {
                self.handle_allow_writes(id);
            }
        }

        timer.observe_duration();
    }

    fn handle_create_instance(&mut self, config: InstanceConfig) {
        // Seed the worker configuration with the controller's snapshot before applying it, so
        // create-time setup observes controller-synced values rather than dyncfg defaults. The
        // same values arrive again in the following `UpdateConfiguration`, which applies globally
        // and keeps the configuration current. An empty snapshot leaves the defaults in place.
        config
            .initial_config
            .apply(&self.compute_state.worker_config);

        // Ensure the state is consistent with the config before we initialize anything.
        self.compute_state.apply_worker_config();

        // Apply dictionary compression exactly once, here at instance creation, from the value the
        // controller captured when the replica was created. We deliberately do NOT re-apply it on
        // `handle_update_configuration`, so flipping the flag does not retroactively change this
        // replica's arrangements. `DICTIONARY_COMPRESSION` is process-global and a replica process
        // hosts a single instance, so this single store covers all of the replica's arrangements.
        mz_row_spine::DICTIONARY_COMPRESSION.store(
            config.arrangement_dictionary_compression,
            std::sync::atomic::Ordering::Relaxed,
        );

        if let Some(offset) = config.expiration_offset {
            self.compute_state.apply_expiration_offset(offset);
        }

        let storage_log_reader = self.compute_state.storage_log_reader.take();
        self.initialize_logging(config.logging, storage_log_reader);

        self.compute_state.peek_stash_persist_location = Some(config.peek_stash_persist_location);
    }

    fn handle_update_configuration(&mut self, params: ComputeParameters) {
        debug!("Applying configuration update: {params:?}");

        let ComputeParameters {
            workload_class,
            max_result_size,
            tracing,
            grpc_client: _grpc_client,
            dyncfg_updates,
        } = params;

        if let Some(v) = workload_class {
            self.compute_state.metrics.set_workload_class(v);
        }
        if let Some(v) = max_result_size {
            self.compute_state.max_result_size = v;
        }

        tracing.apply(self.compute_state.tracing_handle.as_ref());

        dyncfg_updates.apply(&self.compute_state.worker_config);
        self.compute_state
            .persist_clients
            .cfg()
            .apply_from(&dyncfg_updates);

        // Note: We're only updating mz_metrics from the compute state here, but not from the
        // equivalent storage state. This is because they're running on the same process and
        // share the metrics.
        mz_metrics::update_dyncfg(&dyncfg_updates);

        self.compute_state.apply_worker_config();
    }

    fn handle_create_dataflow(
        &mut self,
        dataflow: DataflowDescription<RenderPlan, CollectionMetadata>,
    ) {
        let dataflow_index = Rc::new(self.timely_worker.next_dataflow_index());
        let as_of = dataflow.as_of.clone().unwrap();

        let dataflow_expiration = dataflow
            .time_dependence
            .as_ref()
            .map(|time_dependence| {
                self.determine_dataflow_expiration(time_dependence, &dataflow.until)
            })
            .unwrap_or_default();

        // Add the dataflow expiration to `until`.
        let until = dataflow.until.meet(&dataflow_expiration);

        if dataflow.is_transient() {
            debug!(
                name = %dataflow.debug_name,
                import_ids = %dataflow.display_import_ids(),
                export_ids = %dataflow.display_export_ids(),
                as_of = ?as_of.elements(),
                time_dependence = ?dataflow.time_dependence,
                expiration = ?dataflow_expiration.elements(),
                expiration_datetime = ?dataflow_expiration
                    .as_option()
                    .map(|t| mz_ore::now::to_datetime(t.into())),
                plan_until = ?dataflow.until.elements(),
                until = ?until.elements(),
                "creating dataflow",
            );
        } else {
            info!(
                name = %dataflow.debug_name,
                import_ids = %dataflow.display_import_ids(),
                export_ids = %dataflow.display_export_ids(),
                as_of = ?as_of.elements(),
                time_dependence = ?dataflow.time_dependence,
                expiration = ?dataflow_expiration.elements(),
                expiration_datetime = ?dataflow_expiration
                    .as_option()
                    .map(|t| mz_ore::now::to_datetime(t.into())),
                plan_until = ?dataflow.until.elements(),
                until = ?until.elements(),
                "creating dataflow",
            );
        };

        let subscribe_copy_ids: BTreeSet<_> = dataflow
            .subscribe_ids()
            .chain(dataflow.copy_to_ids())
            .collect();

        // `StartSignal` is attached only to imported sources and imported indexes, and
        // `import_ids` is exactly those two sets, so a dataflow with no imports has nothing
        // suspended and begins computing as soon as it is rendered. Such a dataflow can reach
        // hydration before its `Schedule` arrives, and the controller sends one anyway to keep
        // protocol communication predictable, so a `started_at` stamped only from
        // `handle_schedule` would land after `hydrated_at`. Stamping it here also keeps the row
        // truthful from the moment it appears, rather than reporting the object as queued while
        // nothing is queueing it.
        let starts_immediately = dataflow.import_ids().next().is_none();

        // Initialize compute and logging state for each object.
        for object_id in dataflow.export_ids() {
            let is_subscribe_or_copy = subscribe_copy_ids.contains(&object_id);
            let metrics = self.compute_state.metrics.for_collection(object_id);
            let mut collection = CollectionState::new(
                Rc::clone(&dataflow_index),
                is_subscribe_or_copy,
                as_of.clone(),
                metrics,
            );

            if let Some(logger) = self.compute_state.compute_logger.clone() {
                let logging = CollectionLogging::new(
                    object_id,
                    logger,
                    *dataflow_index,
                    dataflow.import_ids(),
                );
                if starts_immediately {
                    logging.set_hydration_start();
                }
                collection.logging = Some(logging);
            }

            collection.reset_reported_frontiers(ReportedFrontier::NotReported {
                lower: as_of.clone(),
            });

            let existing = self.compute_state.collections.insert(object_id, collection);
            if existing.is_some() {
                error!(
                    id = ?object_id,
                    "existing collection for newly created dataflow",
                );
            }
        }

        let (start_signal, suspension_token) = StartSignal::new();
        for id in dataflow.export_ids() {
            self.compute_state
                .suspended_collections
                .insert(id, Rc::clone(&suspension_token));
        }

        crate::render::build_compute_dataflow(
            self.timely_worker,
            self.compute_state,
            dataflow,
            start_signal,
            until,
            dataflow_expiration,
        );
    }

    fn handle_schedule(&mut self, id: GlobalId) {
        // A `Schedule` command instructs us to begin dataflow computation for a collection, so
        // we should unsuspend it by dropping the corresponding suspension token. Note that a
        // dataflow can export multiple collections and they all share one suspension token, so the
        // computation of a dataflow will only start once all its exported collections have been
        // scheduled.
        let suspension_token = self.compute_state.suspended_collections.remove(&id);
        drop(suspension_token);

        if let Some(collection) = self.compute_state.collections.get(&id) {
            if let Some(logging) = &collection.logging {
                logging.set_hydration_start();
            }
        }
    }

    fn handle_allow_compaction(&mut self, id: GlobalId, frontier: Antichain<Timestamp>) {
        if frontier.is_empty() {
            // Indicates that we may drop `id`, as there are no more valid times to read.
            self.drop_collection(id);
        } else {
            self.compute_state
                .traces
                .allow_compaction(id, frontier.borrow());
        }
    }

    #[mz_ore::instrument(level = "debug")]
    fn handle_peek(&mut self, peek: Peek) {
        let pending = match &peek.target {
            PeekTarget::Index { id } => {
                // Acquire a copy of the trace suitable for fulfilling the peek.
                let trace_bundle = self.compute_state.traces.get(id).unwrap().clone();
                PendingPeek::index(peek, trace_bundle)
            }
            PeekTarget::Persist { metadata, .. } => {
                let metadata = metadata.clone();
                PendingPeek::persist(
                    peek,
                    Arc::clone(&self.compute_state.persist_clients),
                    metadata,
                    usize::cast_from(self.compute_state.max_result_size),
                    self.timely_worker,
                    PeekRowIterationConfig::new(&self.compute_state.worker_config),
                )
            }
        };

        // Log the receipt of the peek.
        if let Some(logger) = self.compute_state.compute_logger.as_mut() {
            logger.log(&pending.as_log_event(true));
        }

        self.process_peek(&mut Antichain::new(), pending);

        // `process_peek` may leave this peek waiting for budget, and no sweep of the pending peeks
        // follows on this path. `reconcile` applies the peeks a reconnecting controller re-sends
        // and then returns into the worker loop, which parks before its next sweep, so a peek
        // deferred here would wait on an activation that nothing else produces. The park is
        // unbounded when the maintenance interval is zero, which is a supported setting.
        if self.compute_state.peek_resume_at.is_some() {
            self.request_peek_activation();
        }
    }

    fn handle_cancel_peek(&mut self, uuid: Uuid) {
        if let Some(peek) = self.compute_state.pending_peeks.remove(&uuid) {
            self.send_peek_response(peek, PeekResponse::Canceled);
        }
    }

    fn handle_allow_writes(&mut self, id: GlobalId) {
        // Enable persist compaction on any allow-writes command. We
        // assume persist only compacts after making durable changes,
        // such as appending a batch or advancing the upper.
        self.compute_state.persist_clients.cfg().enable_compaction();

        if let Some(collection) = self.compute_state.collections.get_mut(&id) {
            collection.allow_writes();
        } else {
            soft_panic_or_log!("allow writes for unknown collection {id}");
        }
    }

    /// Drop the given collection.
    fn drop_collection(&mut self, id: GlobalId) {
        let collection = self
            .compute_state
            .collections
            .remove(&id)
            .expect("dropped untracked collection");

        // If this collection is an index, remove its trace.
        self.compute_state.traces.remove(&id);
        // If the collection is unscheduled, remove it from the list of waiting collections.
        self.compute_state.suspended_collections.remove(&id);

        // Drop the dataflow, if all its exports have been dropped.
        if let Ok(index) = Rc::try_unwrap(collection.dataflow_index) {
            self.timely_worker.drop_dataflow(index);
        }

        // The compute protocol requires us to send a `Frontiers` response with empty frontiers
        // when a collection was dropped, unless:
        //  * The frontier was already reported as empty previously, or
        //  * The collection is a subscribe or copy-to.
        if !collection.is_subscribe_or_copy {
            let reported = collection.reported_frontiers;
            let write_frontier = (!reported.write_frontier.is_empty()).then(Antichain::new);
            let input_frontier = (!reported.input_frontier.is_empty()).then(Antichain::new);
            let output_frontier = (!reported.output_frontier.is_empty()).then(Antichain::new);

            let frontiers = FrontiersResponse {
                write_frontier,
                input_frontier,
                output_frontier,
            };
            if frontiers.has_updates() {
                self.send_compute_response(ComputeResponse::Frontiers(id, frontiers));
            }
        }
    }

    /// Initializes timely dataflow logging and publishes as a view.
    pub fn initialize_logging(
        &mut self,
        config: LoggingConfig,
        storage_log_reader: Option<crate::server::StorageTimelyLogReader>,
    ) {
        if self.compute_state.compute_logger.is_some() {
            panic!("dataflow server has already initialized logging");
        }

        let LoggingTraces {
            traces,
            dataflow_index,
            compute_logger: logger,
        } = logging::initialize(
            self.timely_worker,
            &config,
            self.compute_state.metrics_registry.clone(),
            Rc::clone(&self.compute_state.worker_config),
            self.compute_state.workers_per_process,
            storage_log_reader,
        );

        let dataflow_index = Rc::new(dataflow_index);
        let mut log_index_ids = config.index_logs;
        for (log, trace) in traces {
            // Install trace as maintained index.
            let id = log_index_ids
                .remove(&log)
                .expect("`logging::initialize` does not invent logs");
            self.compute_state.traces.set(id, trace);

            // Initialize compute and logging state for the logging index.
            let is_subscribe_or_copy = false;
            let as_of = Antichain::from_elem(Timestamp::MIN);
            let metrics = self.compute_state.metrics.for_collection(id);
            let mut collection = CollectionState::new(
                Rc::clone(&dataflow_index),
                is_subscribe_or_copy,
                as_of,
                metrics,
            );

            let logging =
                CollectionLogging::new(id, logger.clone(), *dataflow_index, std::iter::empty());
            // Log collections are never suspended and the controller marks them scheduled
            // implicitly, so no `Schedule` command ever arrives for them. Record their hydration
            // start here, or they would sit permanently in the illegal state of being hydrated
            // without having started.
            logging.set_hydration_start();
            collection.logging = Some(logging);

            let existing = self.compute_state.collections.insert(id, collection);
            if existing.is_some() {
                error!(
                    id = ?id,
                    "existing collection for newly initialized logging export",
                );
            }
        }

        // Sanity check.
        assert!(
            log_index_ids.is_empty(),
            "failed to create requested logging indexes: {log_index_ids:?}",
        );

        self.compute_state.compute_logger = Some(logger);
    }

    /// Send progress information to the controller.
    pub fn report_frontiers(&mut self) {
        let mut responses = Vec::new();

        // Maintain a single allocation for `new_frontier` to avoid allocating on every iteration.
        let mut new_frontier = Antichain::new();

        for (&id, collection) in self.compute_state.collections.iter_mut() {
            // The compute protocol does not allow `Frontiers` responses for subscribe and copy-to
            // collections (database-issues#4701).
            if collection.is_subscribe_or_copy {
                continue;
            }

            let reported = collection.reported_frontiers();

            // Collect the write frontier and check for progress.
            new_frontier.clear();
            if let Some(traces) = self.compute_state.traces.get_mut(&id) {
                assert!(
                    collection.sink_write_frontier.is_none(),
                    "collection {id} has multiple frontiers"
                );
                traces.oks_mut().read_upper(&mut new_frontier);
            } else if let Some(frontier) = &collection.sink_write_frontier {
                new_frontier.clone_from(&frontier.borrow());
            } else {
                error!(id = ?id, "collection without write frontier");
                continue;
            }
            let new_write_frontier = reported
                .write_frontier
                .allows_reporting(&new_frontier)
                .then(|| new_frontier.clone());

            // Collect the output frontier and check for progress.
            //
            // By default, the output frontier equals the write frontier (which is still stored in
            // `new_frontier`). If the collection provides a compute frontier, we construct the
            // output frontier by taking the meet of write and compute frontier, to avoid:
            //  * reporting progress through times we have not yet written
            //  * reporting progress through times we have not yet fully processed, for
            //    collections that jump their write frontiers into the future
            //
            // As a special case, in read-only mode we don't take the write frontier into account.
            // The dataflow doesn't have the ability to push it forward, so it can't be used as a
            // measure of dataflow progress.
            if let Some(probe) = &collection.compute_probe {
                if *collection.read_only_rx.borrow() {
                    new_frontier.clear();
                }
                probe.with_frontier(|frontier| new_frontier.extend(frontier.iter().copied()));
            }
            let new_output_frontier = reported
                .output_frontier
                .allows_reporting(&new_frontier)
                .then(|| new_frontier.clone());

            // Collect the input frontier and check for progress.
            new_frontier.clear();
            for probe in collection.input_probes.values() {
                probe.with_frontier(|frontier| new_frontier.extend(frontier.iter().copied()));
            }
            let new_input_frontier = reported
                .input_frontier
                .allows_reporting(&new_frontier)
                .then(|| new_frontier.clone());

            if let Some(frontier) = &new_write_frontier {
                collection
                    .set_reported_write_frontier(ReportedFrontier::Reported(frontier.clone()));
            }
            if let Some(frontier) = &new_input_frontier {
                collection
                    .set_reported_input_frontier(ReportedFrontier::Reported(frontier.clone()));
            }
            if let Some(frontier) = &new_output_frontier {
                collection
                    .set_reported_output_frontier(ReportedFrontier::Reported(frontier.clone()));
            }

            let response = FrontiersResponse {
                write_frontier: new_write_frontier,
                input_frontier: new_input_frontier,
                output_frontier: new_output_frontier,
            };
            if response.has_updates() {
                responses.push((id, response));
            }
        }

        for (id, frontiers) in responses {
            self.send_compute_response(ComputeResponse::Frontiers(id, frontiers));
        }
    }

    /// Report per-worker metrics.
    pub(crate) fn report_metrics(&self) {
        if let Some(expiration) = self.compute_state.replica_expiration.as_option() {
            let now = Duration::from_millis(mz_ore::now::SYSTEM_TIME()).as_secs_f64();
            let expiration = Duration::from_millis(<u64>::from(expiration)).as_secs_f64();
            let remaining = expiration - now;
            self.compute_state
                .metrics
                .replica_expiration_remaining_seconds
                .set(remaining)
        }
    }

    /// Either complete the peek (and send the response) or put it in the pending set.
    ///
    /// An index peek walks for as long as this activation's remaining budget allows, and a peek
    /// that is granted none of it is left pending untouched, to be served first on the next
    /// activation.
    fn process_peek(&mut self, upper: &mut Antichain<Timestamp>, mut peek: PendingPeek) {
        // Asked for ahead of the match rather than in a guard on its arms, because the grant is
        // what arms this activation's budget. Only an index peek draws on that budget, so a peek
        // of any other kind must not arm it.
        let granted = match &peek {
            PendingPeek::Index(_) => self.compute_state.peek_budget.grant(),
            _ => None,
        };

        let response = match &mut peek {
            PendingPeek::Index(peek) if granted.is_none() => {
                // This activation has spent what it may on peeks. Nothing about the peek has
                // changed, because a scan is opened by the slice that walks it, so passing it over
                // costs the peek an activation and nothing else.
                self.compute_state
                    .peek_resume_at
                    .get_or_insert(peek.peek.uuid);
                None
            }
            PendingPeek::Index(peek) => {
                let start = Instant::now();
                let granted = granted.expect("guarded by the preceding arm");

                let row_iteration_limit =
                    peek_row_iteration_limit(&self.compute_state.worker_config);

                let peek_stash_eligible = peek
                    .peek
                    .finishing
                    .is_streamable(peek.peek.result_desc.arity());

                let peek_stash_enabled = {
                    let enabled = ENABLE_PEEK_RESPONSE_STASH.get(&self.compute_state.worker_config);
                    let peek_persist_stash_available =
                        self.compute_state.peek_stash_persist_location.is_some();
                    if !peek_persist_stash_available && enabled {
                        error!("missing peek_stash_persist_location but peek stash is enabled");
                    }
                    enabled && peek_persist_stash_available
                };

                let peek_stash_threshold_bytes =
                    PEEK_RESPONSE_STASH_THRESHOLD_BYTES.get(&self.compute_state.worker_config);

                let metrics = IndexPeekMetrics {
                    seek_fulfillment_seconds: &self
                        .compute_state
                        .metrics
                        .index_peek_seek_fulfillment_seconds,
                    frontier_check_seconds: &self
                        .compute_state
                        .metrics
                        .index_peek_frontier_check_seconds,
                    walk: &self.compute_state.peek_walk_metrics,
                };

                let mut fuel = granted;
                let status = peek.seek_fulfillment(
                    upper,
                    self.compute_state.max_result_size,
                    peek_stash_enabled && peek_stash_eligible,
                    peek_stash_threshold_bytes,
                    row_iteration_limit,
                    &mut fuel,
                    &metrics,
                );

                // Charged with what the slice walked rather than with what it was granted, so a
                // peek that answers in three positions leaves the activation's budget to the peeks
                // behind it.
                self.compute_state.peek_budget.charge(granted - fuel);

                self.compute_state
                    .metrics
                    .index_peek_total_seconds
                    .observe(start.elapsed().as_secs_f64());

                match status {
                    PeekStatus::Ready(result) => Some(result),
                    PeekStatus::NotReady => None,
                    PeekStatus::Promote(scan) => {
                        let _span =
                            span!(parent: &peek.span, Level::DEBUG, "promote_index_peek").entered();

                        let uuid = peek.peek.uuid;
                        let permits = Arc::clone(&self.compute_state.peek_permits);
                        let offloaded = OffloadedPeek::promote(
                            peek.peek.clone(),
                            peek.trace_bundle.clone(),
                            scan,
                            &permits,
                            OffloadConfig::new(&self.compute_state.worker_config),
                            self.compute_state.peek_walk_metrics.clone(),
                            self.timely_worker.sync_activator_for([].into()),
                        );

                        self.compute_state
                            .pending_peeks
                            .insert(uuid, PendingPeek::Offloaded(offloaded));
                        return;
                    }
                    PeekStatus::UsePeekStash => {
                        let _span =
                            span!(parent: &peek.span, Level::DEBUG, "process_stash_peek").entered();

                        let uuid = peek.peek.uuid;
                        let stash_task = self
                            .start_stash_upload(peek.peek.clone(), peek.trace_bundle.clone())
                            .expect("stash location established before diverting");

                        self.compute_state
                            .pending_peeks
                            .insert(uuid, PendingPeek::Stash(stash_task));
                        return;
                    }
                }
            }
            PendingPeek::Persist(peek) => peek.result.try_recv().ok().map(|(result, duration)| {
                self.compute_state
                    .metrics
                    .persist_peek_seconds
                    .observe(duration.as_secs_f64());
                result
            }),
            PendingPeek::Offloaded(offloaded) => match offloaded.result.try_recv() {
                Ok((outcome, duration)) => {
                    // Both outcomes are timed, because both measure the same thing: how long the
                    // peek was away from the worker, the wait for a permit included. A walk that
                    // hands back is the expensive case rather than an aborted one.
                    self.compute_state
                        .metrics
                        .index_peek_offload_seconds
                        .observe(duration.as_secs_f64());

                    match outcome {
                        OffloadOutcome::Answered(response) => {
                            trace!(?offloaded.peek, ?duration, "finished offloaded index peek walk");
                            Some(response)
                        }
                        OffloadOutcome::NeedsStash => {
                            let _span =
                                span!(parent: &offloaded.span, Level::DEBUG, "process_stash_peek")
                                    .entered();
                            trace!(?offloaded.peek, ?duration, "handing offloaded index peek to the stash");

                            let uuid = offloaded.peek.uuid;
                            let stash_task = self.start_stash_upload(
                                offloaded.peek.clone(),
                                offloaded.trace_bundle.clone(),
                            );

                            match stash_task {
                                Some(stash_task) => {
                                    self.compute_state
                                        .pending_peeks
                                        .insert(uuid, PendingPeek::Stash(stash_task));
                                    return;
                                }
                                None => Some(PeekResponse::Error(PeekError::unstructured(
                                    "peek result is too large to answer inline and this replica \
                                     has no peek stash location",
                                ))),
                            }
                        }
                    }
                }
                Err(oneshot::error::TryRecvError::Empty) => None,
                // The task drops its sender without sending only when it stops without an outcome,
                // and the one way it does that is a cancellation that removed this entry, so an
                // entry that is still here to be polled means the task died. Answering the peek is
                // what keeps it from waiting forever on a walk nothing is running.
                //
                // NOTE: a walk whose task is dropped by a shutting-down tokio runtime reaches here
                // the same way a panicking one does. The worker is going away too in that case, so
                // the log line is noise rather than a lost signal, and the alternative of staying
                // silent would hide a panicked walk in the case that matters.
                Err(oneshot::error::TryRecvError::Closed) => {
                    soft_panic_or_log!(
                        "offloaded walk of peek on {} ended without an outcome",
                        offloaded.peek.target.id()
                    );
                    Some(PeekResponse::Error(PeekError::unstructured(
                        "offloaded peek walk failed",
                    )))
                }
            },
            PendingPeek::Stash(stashing_peek) => {
                let num_batches = PEEK_STASH_NUM_BATCHES.get(&self.compute_state.worker_config);
                let batch_size = PEEK_STASH_BATCH_SIZE.get(&self.compute_state.worker_config);
                stashing_peek.pump_rows(num_batches, batch_size);

                if let Ok((response, duration)) = stashing_peek.result.try_recv() {
                    self.compute_state
                        .metrics
                        .stashed_peek_seconds
                        .observe(duration.as_secs_f64());
                    trace!(?stashing_peek.peek, ?duration, "finished stashing peek response in persist");

                    Some(response)
                } else {
                    None
                }
            }
        };

        if let Some(response) = response {
            let _span = span!(parent: peek.span(), Level::DEBUG, "process_peek_response").entered();
            self.send_peek_response(peek, response)
        } else {
            let uuid = peek.peek().uuid;
            self.compute_state.pending_peeks.insert(uuid, peek);
        }
    }

    /// Starts a peek stash upload for `peek`, which walks the ok trace of `trace_bundle` and
    /// writes the rows it produces to persist.
    ///
    /// The walk starts over: the stash reads the trace bundle itself, so rows a previous walk of
    /// the same peek accumulated are produced again rather than carried across.
    ///
    /// Returns `None` when this replica has no stash location, which a caller must establish
    /// before it decides to divert a peek here.
    fn start_stash_upload(
        &self,
        peek: Peek,
        trace_bundle: TraceBundle,
    ) -> Option<peek_stash::StashingPeek> {
        let persist_location = self.compute_state.peek_stash_persist_location.clone()?;

        // NOTE: The row iteration limit does not follow a peek into the stash. The stash restarts
        // the scan and produces in bounded bursts, so a stashed peek may examine any number of
        // rows.
        let batch_max_runs =
            PEEK_RESPONSE_STASH_BATCH_MAX_RUNS.get(&self.compute_state.worker_config);

        Some(peek_stash::StashingPeek::start_upload(
            Arc::clone(&self.compute_state.persist_clients),
            &persist_location,
            peek,
            trace_bundle,
            batch_max_runs,
        ))
    }

    /// Scan pending peeks and attempt to retire each.
    pub fn process_peeks(&mut self) {
        // Begun ahead of the early return below, because the aggregate bounds an activation and
        // this is the only place an activation begins. A replica whose index peeks all answer
        // inline leaves none of them pending, so a sweep that began an activation only where it
        // found work would let the aggregate drain across the arrivals such a replica serves and
        // then pass every later arrival over. Beginning one costs a write of `None`, which is what
        // keeps it above the return.
        self.compute_state.peek_budget.start_activation();

        // What follows runs on every iteration of the worker loop, tens of thousands of times a
        // second on a busy worker, and the overwhelming majority of those find no pending peek.
        // Returning before the map is walked, ordered, or taken keeps that case as cheap as the
        // kill switch path has to be.
        if self.compute_state.pending_peeks.is_empty() {
            // A resume point names where in the uuid ordering the next sweep starts, and with
            // nothing pending there is nothing for it to name. Cancellation and `reconcile` both
            // empty the map without clearing it, and only a sweep consumes one, so a resume point
            // left here would outlive every peek it could serve and make each later arrival ask
            // for an activation with nothing to do.
            self.compute_state.peek_resume_at = None;
            return;
        }

        let mut upper = Antichain::new();
        let mut pending_peeks = std::mem::take(&mut self.compute_state.pending_peeks);

        // Turning the ring costs a vector of the pending uuids and a lookup per peek, where taking
        // the map in its own order costs neither. Only a sweep that passed a peek over leaves a
        // resume point, and no peek is ever passed over while the offload is off, so the kill
        // switch takes the second arm and pays exactly what the worker paid before.
        match self.compute_state.peek_resume_at.take() {
            Some(resume_at) => {
                let mut order: Vec<Uuid> = pending_peeks.keys().copied().collect();
                let resume_from = order.partition_point(|uuid| *uuid < resume_at);
                order.rotate_left(resume_from);

                for uuid in order {
                    let peek = pending_peeks.remove(&uuid).expect("taken from this map");
                    self.process_peek(&mut upper, peek);
                }
            }
            None => {
                for (_uuid, peek) in pending_peeks {
                    self.process_peek(&mut upper, peek);
                }
            }
        }

        if self.compute_state.peek_resume_at.is_some() {
            self.request_peek_activation();
        }
    }

    /// Asks timely to activate this worker again, so that a peek passed over for want of budget
    /// gets its turn.
    ///
    /// Nothing else wakes a worker that passed a peek over. The peeks that spent the budget were
    /// answered or promoted, and neither leaves an activation behind, so a worker with no dataflow
    /// to run would park with peeks waiting on nothing but their turn. Every path that defers a
    /// peek therefore calls this before it hands the worker back.
    fn request_peek_activation(&self) {
        match self.timely_worker.sync_activator_for([].into()).activate() {
            Ok(()) => {}
            Err(_) => debug!("unable to wake timely for peeks left waiting on their turn"),
        }
    }

    /// Sends a response for this peek's resolution to the coordinator.
    ///
    /// Note that this function takes ownership of the `PendingPeek`, which is
    /// meant to prevent multiple responses to the same peek.
    #[mz_ore::instrument(level = "debug")]
    fn send_peek_response(&mut self, peek: PendingPeek, response: PeekResponse) {
        let log_event = peek.as_log_event(false);
        // Respond with the response.
        self.send_compute_response(ComputeResponse::PeekResponse(
            peek.peek().uuid,
            response,
            OpenTelemetryContext::obtain(),
        ));

        // Log responding to the peek request.
        if let Some(logger) = self.compute_state.compute_logger.as_mut() {
            logger.log(&log_event);
        }
    }

    /// Scan the shared subscribe response buffer, and forward results along.
    pub fn process_subscribes(&mut self) {
        let mut subscribe_responses = self.compute_state.subscribe_response_buffer.borrow_mut();
        for (sink_id, mut response) in subscribe_responses.drain(..) {
            // Update frontier logging for this subscribe.
            if let Some(collection) = self.compute_state.collections.get_mut(&sink_id) {
                let new_frontier = match &response {
                    SubscribeResponse::Batch(b) => b.upper.clone(),
                    SubscribeResponse::DroppedAt(_) => Antichain::new(),
                };

                let reported = collection.reported_frontiers();
                assert!(
                    reported.write_frontier.allows_reporting(&new_frontier),
                    "subscribe write frontier regression: {:?} -> {:?}",
                    reported.write_frontier,
                    new_frontier,
                );
                assert!(
                    reported.input_frontier.allows_reporting(&new_frontier),
                    "subscribe input frontier regression: {:?} -> {:?}",
                    reported.input_frontier,
                    new_frontier,
                );

                collection
                    .set_reported_write_frontier(ReportedFrontier::Reported(new_frontier.clone()));
                collection
                    .set_reported_input_frontier(ReportedFrontier::Reported(new_frontier.clone()));
                collection.set_reported_output_frontier(ReportedFrontier::Reported(new_frontier));
            } else {
                // Presumably tracking state for this subscribe was already dropped by
                // `drop_collection`. There is nothing left to do for logging.
            }

            response
                .to_error_if_exceeds(usize::try_from(self.compute_state.max_result_size).unwrap());
            self.send_compute_response(ComputeResponse::SubscribeResponse(sink_id, response));
        }
    }

    /// Scan the shared copy to response buffer, and forward results along.
    pub fn process_copy_tos(&self) {
        let mut responses = self.compute_state.copy_to_response_buffer.borrow_mut();
        for (sink_id, response) in responses.drain(..) {
            self.send_compute_response(ComputeResponse::CopyToResponse(sink_id, response));
        }
    }

    /// Send a response to the coordinator.
    fn send_compute_response(&self, response: ComputeResponse) {
        // Ignore send errors because the coordinator is free to ignore our
        // responses. This happens during shutdown.
        let _ = self.response_tx.send(response);
    }

    /// Checks for dataflow expiration. Panics if we're past the replica expiration time.
    pub(crate) fn check_expiration(&self) {
        let now = mz_ore::now::SYSTEM_TIME();
        if self.compute_state.replica_expiration.less_than(&now.into()) {
            let now_datetime = mz_ore::now::to_datetime(now);
            let expiration_datetime = self
                .compute_state
                .replica_expiration
                .as_option()
                .map(Into::into)
                .map(mz_ore::now::to_datetime);

            // We error and assert separately to produce structured logs in anything that depends
            // on tracing.
            error!(
                now,
                now_datetime = ?now_datetime,
                expiration = ?self.compute_state.replica_expiration.elements(),
                expiration_datetime = ?expiration_datetime,
                "replica expired"
            );

            // Repeat condition for better error message.
            assert!(
                !self.compute_state.replica_expiration.less_than(&now.into()),
                "replica expired. now: {now} ({now_datetime:?}), expiration: {:?} ({expiration_datetime:?})",
                self.compute_state.replica_expiration.elements(),
            );
        }
    }

    /// Returns the dataflow expiration, i.e, the timestamp beyond which diffs can be
    /// dropped.
    ///
    /// Returns an empty timestamp if `replica_expiration` is unset or matches conditions under
    /// which dataflow expiration should be disabled.
    pub fn determine_dataflow_expiration(
        &self,
        time_dependence: &TimeDependence,
        until: &Antichain<Timestamp>,
    ) -> Antichain<Timestamp> {
        // Evaluate time dependence with respect to the expiration time.
        // * Step time forward to ensure the expiration time is different to the moment a dataflow
        //   can legitimately jump to.
        // * We cannot expire dataflow with an until that is less or equal to the expiration time.
        let iter = self
            .compute_state
            .replica_expiration
            .iter()
            .filter_map(|t| time_dependence.apply(*t))
            .filter_map(|t| Timestamp::try_step_forward(&t))
            .filter(|expiration| !until.less_equal(expiration));
        Antichain::from_iter(iter)
    }
}

/// A peek against either an index or a Persist collection.
///
/// Note that `PendingPeek` intentionally does not implement or derive `Clone`,
/// as each `PendingPeek` is meant to be dropped after it's responded to.
pub enum PendingPeek {
    /// A peek against an index. (Possibly a temporary index created for the purpose.)
    Index(IndexPeek),
    /// A peek against a Persist-backed collection.
    Persist(PersistPeek),
    /// A peek against an index that is being stashed in the peek stash by an
    /// async background task.
    Stash(peek_stash::StashingPeek),
    /// A peek against an index whose walk was promoted off the worker and is running as an async
    /// task.
    Offloaded(OffloadedPeek),
}

impl PendingPeek {
    /// Produces a corresponding log event.
    pub fn as_log_event(&self, installed: bool) -> ComputeEvent {
        let peek = self.peek();
        let (id, peek_type) = match &peek.target {
            PeekTarget::Index { id } => (*id, logging::compute::PeekType::Index),
            PeekTarget::Persist { id, .. } => (*id, logging::compute::PeekType::Persist),
        };
        let uuid = peek.uuid.into_bytes();
        ComputeEvent::Peek(PeekEvent {
            id,
            time: peek.timestamp,
            uuid,
            peek_type,
            installed,
        })
    }

    fn index(peek: Peek, mut trace_bundle: TraceBundle) -> Self {
        let empty_frontier = Antichain::new();
        let timestamp_frontier = Antichain::from_elem(peek.timestamp);
        trace_bundle
            .oks_mut()
            .set_logical_compaction(timestamp_frontier.borrow());
        trace_bundle
            .errs_mut()
            .set_logical_compaction(timestamp_frontier.borrow());
        trace_bundle
            .oks_mut()
            .set_physical_compaction(empty_frontier.borrow());
        trace_bundle
            .errs_mut()
            .set_physical_compaction(empty_frontier.borrow());

        PendingPeek::Index(IndexPeek {
            peek,
            trace_bundle,
            span: tracing::Span::current(),
        })
    }

    fn persist(
        peek: Peek,
        persist_clients: Arc<PersistClientCache>,
        metadata: CollectionMetadata,
        max_result_size: usize,
        timely_worker: &TimelyWorker,
        row_iteration_config: PeekRowIterationConfig,
    ) -> Self {
        let active_worker = {
            // Choose the worker that does the actual peek arbitrarily but consistently.
            let chosen_index = usize::cast_from(peek.uuid.hashed()) % timely_worker.peers();
            chosen_index == timely_worker.index()
        };
        let activator = timely_worker.sync_activator_for([].into());
        let peek_uuid = peek.uuid;

        let (result_tx, result_rx) = oneshot::channel();
        let timestamp = peek.timestamp;
        let mfp_plan = peek.map_filter_project.clone();
        let max_results_needed = peek
            .finishing
            .limit
            .map(|l| usize::cast_from(u64::from(l)))
            .unwrap_or(usize::MAX)
            + peek.finishing.offset;
        let order_by = peek.finishing.order_by.clone();

        // Persist peeks can include at most one literal constraint.
        let literal_constraint = peek
            .literal_constraints
            .clone()
            .map(|rows| rows.into_element());

        let task_handle = mz_ore::task::spawn(|| "persist::peek", async move {
            let start = Instant::now();
            let result = if active_worker {
                PersistPeek::do_peek(
                    &persist_clients,
                    metadata,
                    timestamp,
                    literal_constraint,
                    mfp_plan,
                    max_result_size,
                    max_results_needed,
                    row_iteration_config,
                )
                .await
            } else {
                Ok(vec![])
            };
            let result = match result {
                Ok(rows) => PeekResponse::Rows(vec![RowCollection::new(rows, &order_by)]),
                Err(error) => PeekResponse::Error(error),
            };
            match result_tx.send((result, start.elapsed())) {
                Ok(()) => {}
                Err((_result, elapsed)) => {
                    debug!(duration =? elapsed, "dropping result for cancelled peek {peek_uuid}")
                }
            }
            match activator.activate() {
                Ok(()) => {}
                Err(_) => {
                    debug!("unable to wake timely after completed peek {peek_uuid}");
                }
            }
        });
        PendingPeek::Persist(PersistPeek {
            peek,
            _abort_handle: task_handle.abort_on_drop(),
            result: result_rx,
            span: tracing::Span::current(),
        })
    }

    fn span(&self) -> &tracing::Span {
        match self {
            PendingPeek::Index(p) => &p.span,
            PendingPeek::Persist(p) => &p.span,
            PendingPeek::Stash(p) => &p.span,
            PendingPeek::Offloaded(p) => &p.span,
        }
    }

    pub(crate) fn peek(&self) -> &Peek {
        match self {
            PendingPeek::Index(p) => &p.peek,
            PendingPeek::Persist(p) => &p.peek,
            PendingPeek::Stash(p) => &p.peek,
            PendingPeek::Offloaded(p) => &p.peek,
        }
    }
}

/// An in-progress Persist peek.
///
/// Note that `PendingPeek` intentionally does not implement or derive `Clone`,
/// as each `PendingPeek` is meant to be dropped after it's responded to.
pub struct PersistPeek {
    pub(crate) peek: Peek,
    /// A background task that's responsible for producing the peek results.
    /// If we're no longer interested in the results, we abort the task.
    _abort_handle: AbortOnDropHandle<()>,
    /// The result of the background task, eventually.
    result: oneshot::Receiver<(PeekResponse, Duration)>,
    /// The `tracing::Span` tracking this peek's operation
    span: tracing::Span,
}

impl PersistPeek {
    async fn do_peek(
        persist_clients: &PersistClientCache,
        metadata: CollectionMetadata,
        as_of: Timestamp,
        literal_constraint: Option<Row>,
        mfp_plan: SafeMfpPlan,
        max_result_size: usize,
        mut limit_remaining: usize,
        row_iteration_config: PeekRowIterationConfig,
    ) -> Result<Vec<(Row, NonZeroUsize)>, PeekError> {
        let client = persist_clients
            .open(metadata.persist_location)
            .await
            .map_err(|e| PeekError::unstructured(e.to_string()))?;

        let mut reader: ReadHandle<SourceData, (), Timestamp, StorageDiff> = client
            .open_leased_reader(
                metadata.data_shard,
                Arc::new(metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics::from_purpose("persist::peek"),
                USE_CRITICAL_SINCE_SNAPSHOT.get(client.dyncfgs()),
            )
            .await
            .map_err(|e| PeekError::unstructured(e.to_string()))?;

        // If we are using txn-wal for this collection, then the upper might
        // be advanced lazily and we have to go through txn-wal for reads.
        //
        // TODO: If/when we have a process-wide TxnsRead worker for clusterd,
        // use in here (instead of opening a new TxnsCache) to save a persist
        // reader registration and some txns shard read traffic.
        let mut txns_read = if let Some(txns_id) = metadata.txns_shard {
            Some(TxnsCache::open(&client, txns_id, Some(metadata.data_shard)).await)
        } else {
            None
        };

        let metrics = client.metrics();

        let mut cursor = StatsCursor::new(
            &mut reader,
            txns_read.as_mut(),
            metrics,
            &mfp_plan,
            &metadata.relation_desc,
            Antichain::from_elem(as_of),
        )
        .await
        .map_err(|since| {
            PeekError::unstructured(format!(
                "attempted to peek at {as_of}, but the since has advanced to {since:?}"
            ))
        })?;

        // Re-used state for processing and building rows.
        let mut result = vec![];
        let mut datum_vec = DatumVec::new();
        let mut row_builder = Row::default();
        let arena = RowArena::new();
        let mut total_size = 0usize;
        let mut row_iteration_tracker = PeekRowIterationTracker::new(None, 0);

        let literal_len = match &literal_constraint {
            None => 0,
            Some(row) => row.iter().count(),
        };

        'collect: while limit_remaining > 0 {
            let Some(batch) = cursor.next().await else {
                break;
            };
            for (data, _, d) in batch {
                // Count before literal and MFP filtering because the Persist row
                // has already been read and must still be examined.
                row_iteration_tracker.set_limit(row_iteration_config.current_limit());
                row_iteration_tracker.track_next()?;

                let row = data.map_err(PeekError::from)?;

                if let Some(literal) = &literal_constraint {
                    match row.iter().take(literal_len).cmp(literal.iter()) {
                        Ordering::Less => continue,
                        Ordering::Equal => {}
                        Ordering::Greater => break 'collect,
                    }
                }

                let count: usize = d.try_into().map_err(|_| {
                    error!(
                        shard = %metadata.data_shard, diff = d, ?row,
                        "persist peek encountered negative multiplicities",
                    );
                    PeekError::unstructured(format!(
                        "Invalid data in source, \
                         saw retractions ({}) for row that does not exist: {:?}",
                        -d, row,
                    ))
                })?;
                let Some(count) = NonZeroUsize::new(count) else {
                    continue;
                };
                let mut datum_local = datum_vec.borrow_with(&row);
                let eval_result = mfp_plan
                    .evaluate_into(&mut datum_local, &arena, &mut row_builder)
                    .map(|row| row.cloned())
                    .map_err(PeekError::from)?;
                if let Some(row) = eval_result {
                    total_size = total_size
                        .saturating_add(row.byte_len())
                        .saturating_add(std::mem::size_of::<NonZeroUsize>());
                    if total_size > max_result_size {
                        return Err(PeekError::unstructured(format!(
                            "result exceeds max size of {}",
                            ByteSize::b(u64::cast_from(max_result_size))
                        )));
                    }
                    result.push((row, count));
                    limit_remaining = limit_remaining.saturating_sub(count.get());
                    if limit_remaining == 0 {
                        break;
                    }
                }
            }
        }

        Ok(result)
    }
}

/// An in-progress index-backed peek, and data to eventually fulfill it.
pub struct IndexPeek {
    peek: Peek,
    /// The data from which the trace derives.
    trace_bundle: TraceBundle,
    /// The `tracing::Span` tracking this peek's operation
    span: tracing::Span,
}

impl IndexPeek {
    /// Attempts to fulfill the peek and reports success.
    ///
    /// To produce output at `peek.timestamp`, we must be certain that
    /// it is no longer changing. A trace guarantees that all future
    /// changes will be greater than or equal to an element of `upper`.
    ///
    /// If an element of `upper` is less or equal to `peek.timestamp`,
    /// then there can be further updates that would change the output.
    /// If no element of `upper` is less or equal to `peek.timestamp`,
    /// then for any time `t` less or equal to `peek.timestamp` it is
    /// not the case that `upper` is less or equal to that timestamp,
    /// and so the result cannot further evolve.
    ///
    /// `fuel` bounds how far the walk may go on this worker, in cursor positions, and is charged
    /// for the positions it visits. A walk that exhausts it and still has work left is promoted
    /// rather than continued here. A peek whose frontiers do not yet admit the read spends none of
    /// it.
    fn seek_fulfillment(
        &mut self,
        upper: &mut Antichain<Timestamp>,
        max_result_size: u64,
        peek_stash_eligible: bool,
        peek_stash_threshold_bytes: usize,
        row_iteration_limit: Option<usize>,
        fuel: &mut usize,
        metrics: &IndexPeekMetrics<'_>,
    ) -> PeekStatus {
        let method_start = Instant::now();

        self.trace_bundle.oks_mut().read_upper(upper);
        if upper.less_equal(&self.peek.timestamp) {
            return PeekStatus::NotReady;
        }
        self.trace_bundle.errs_mut().read_upper(upper);
        if upper.less_equal(&self.peek.timestamp) {
            return PeekStatus::NotReady;
        }

        let read_frontier = self.trace_bundle.compaction_frontier();
        if !read_frontier.less_equal(&self.peek.timestamp) {
            let error = format!(
                "Arrangement compaction frontier ({:?}) is beyond the time of the attempted read ({})",
                read_frontier.elements(),
                self.peek.timestamp,
            );
            return PeekStatus::Ready(PeekResponse::Error(PeekError::unstructured(error)));
        }

        metrics
            .frontier_check_seconds
            .observe(method_start.elapsed().as_secs_f64());

        let result = self.collect_finished_data(
            max_result_size,
            peek_stash_eligible,
            peek_stash_threshold_bytes,
            row_iteration_limit,
            fuel,
            metrics,
        );

        metrics
            .seek_fulfillment_seconds
            .observe(method_start.elapsed().as_secs_f64());

        result
    }

    /// Answers the peek by scanning the traces that fulfil it, for as long as `fuel` allows.
    ///
    /// One call opens one scan and either answers from it or hands it on, so nothing survives the
    /// call. A scan that runs out of fuel with work left leaves with the [`PeekStatus::Promote`]
    /// that reports it, which is what keeps the positions it walked from being walked again.
    fn collect_finished_data(
        &mut self,
        max_result_size: u64,
        peek_stash_eligible: bool,
        peek_stash_threshold_bytes: usize,
        row_iteration_limit: Option<usize>,
        fuel: &mut usize,
        metrics: &IndexPeekMetrics<'_>,
    ) -> PeekStatus {
        let peek = &self.peek;
        let (oks, errs) = self.trace_bundle.oks_errs_mut();
        let mut scan = PeekScan::new(
            peek,
            errs,
            oks,
            max_result_size,
            peek_stash_eligible,
            peek_stash_threshold_bytes,
        );

        let outcome = scan.step(row_iteration_limit, fuel);

        // A suspension the offload can resume is the one outcome that leaves the walk unfinished,
        // so it is the one outcome this driver reports nothing for. Everything else ends the walk
        // here, and this driver is the one that accounts for it.
        let promoted = matches!(outcome, ScanOutcome::Suspended) && !scan.batch_ready();
        let phases = scan.phases();
        if !promoted {
            metrics.walk.walked_inline();
            metrics.walk.observe_error_phase(&phases);
        }

        let rows = match outcome {
            ScanOutcome::Complete(rows) => rows,
            ScanOutcome::Failed(error) => return PeekStatus::Ready(PeekResponse::Error(error)),
            // A scan that suspends without a batch has work left and rows it is still allowed to
            // accumulate, so it is resumed rather than disposed of. Every position it has walked
            // travels with it, and so does the account of what those positions cost, which is what
            // makes the promotion cost one hand-off instead of a second walk.
            ScanOutcome::Suspended if !scan.batch_ready() => return PeekStatus::Promote(scan),
            // Diversion is sound only for a scan whose error walk is over. The stash answers the
            // peek from a walk of the ok trace alone and never reads the error trace, so a peek
            // diverted with its error trace half-read would return rows where it must report an
            // error. Only the ok walk accumulates rows, so a scan holding a full batch has read
            // the error trace out, and the guard states that rather than assuming it.
            ScanOutcome::Suspended => {
                if !scan.error_trace_clean() {
                    soft_panic_or_log!(
                        "peek on {} suspended before its error trace was read out",
                        self.peek.target.id()
                    );
                    return PeekStatus::Ready(PeekResponse::Error(PeekError::unstructured(
                        "peek suspended before its error trace was read out",
                    )));
                }
                // The batch is taken and dropped rather than left to the scan's own drop, because
                // discarding it is this driver's decision: the stash walks the ok trace again from
                // the trace bundle and produces these rows a second time.
                let _batch = scan.take_batch();
                return PeekStatus::UsePeekStash;
            }
        };

        metrics.walk.observe_ok_phase(&phases);

        PeekStatus::Ready(
            metrics
                .walk
                .rows_response(rows, &self.peek.finishing.order_by),
        )
    }
}

/// For keeping track of the state of pending or ready peeks, and managing
/// control flow.
enum PeekStatus {
    /// The frontiers of objects are not yet advanced enough, peek is still
    /// pending.
    NotReady,
    /// The result size is above the configured threshold and the peek is
    /// eligible for using the peek result stash.
    UsePeekStash,
    /// The walk stopped with work left and nothing to hand over, so it is finished away from the
    /// worker. Carries the scan, which resumes from the cursor positions it stopped on.
    Promote(IndexPeekScan),
    /// The peek result is ready.
    Ready(PeekResponse),
}

/// The frontiers we have reported to the controller for a collection.
#[derive(Debug)]
struct ReportedFrontiers {
    /// The reported write frontier.
    write_frontier: ReportedFrontier,
    /// The reported input frontier.
    input_frontier: ReportedFrontier,
    /// The reported output frontier.
    output_frontier: ReportedFrontier,
}

impl ReportedFrontiers {
    /// Creates a new `ReportedFrontiers` instance.
    fn new() -> Self {
        Self {
            write_frontier: ReportedFrontier::new(),
            input_frontier: ReportedFrontier::new(),
            output_frontier: ReportedFrontier::new(),
        }
    }
}

/// A frontier we have reported to the controller, or the least frontier we are allowed to report.
#[derive(Clone, Debug)]
pub enum ReportedFrontier {
    /// A frontier has been previously reported.
    Reported(Antichain<Timestamp>),
    /// No frontier has been reported yet.
    NotReported {
        /// A lower bound for frontiers that may be reported in the future.
        lower: Antichain<Timestamp>,
    },
}

impl ReportedFrontier {
    /// Create a new `ReportedFrontier` enforcing the minimum lower bound.
    pub fn new() -> Self {
        let lower = Antichain::from_elem(timely::progress::Timestamp::minimum());
        Self::NotReported { lower }
    }

    /// Whether the reported frontier is the empty frontier.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Reported(frontier) => frontier.is_empty(),
            Self::NotReported { .. } => false,
        }
    }

    /// Whether this `ReportedFrontier` allows reporting the given frontier.
    ///
    /// A `ReportedFrontier` allows reporting of another frontier if:
    ///  * The other frontier is greater than the reported frontier.
    ///  * The other frontier is greater than or equal to the lower bound.
    fn allows_reporting(&self, other: &Antichain<Timestamp>) -> bool {
        match self {
            Self::Reported(frontier) => PartialOrder::less_than(frontier, other),
            Self::NotReported { lower } => PartialOrder::less_equal(lower, other),
        }
    }
}

/// State maintained for a compute collection.
pub struct CollectionState {
    /// Tracks the frontiers that have been reported to the controller.
    reported_frontiers: ReportedFrontiers,
    /// The index of the dataflow computing this collection.
    ///
    /// Used for dropping the dataflow when the collection is dropped.
    /// The Dataflow index is wrapped in an `Rc`s and can be shared between collections, to reflect
    /// the possibility that a single dataflow can export multiple collections.
    dataflow_index: Rc<usize>,
    /// Whether this collection is a subscribe or copy-to.
    ///
    /// The compute protocol does not allow `Frontiers` responses for subscribe and copy-to
    /// collections, so we need to be able to recognize them. This is something we would like to
    /// change in the future (database-issues#4701).
    pub is_subscribe_or_copy: bool,
    /// The collection's initial as-of frontier.
    ///
    /// Used to determine hydration status.
    as_of: Antichain<Timestamp>,

    /// A token that should be dropped when this collection is dropped to clean up associated
    /// sink state.
    ///
    /// Only `Some` if the collection is a sink.
    pub sink_token: Option<SinkToken>,
    /// Frontier of sink writes.
    ///
    /// Only `Some` if the collection is a sink and *not* a subscribe.
    pub sink_write_frontier: Option<Rc<RefCell<Antichain<Timestamp>>>>,
    /// Frontier probes for every input to the collection.
    pub input_probes: BTreeMap<GlobalId, probe::Handle<Timestamp>>,
    /// A probe reporting the frontier of times through which all collection outputs have been
    /// computed (but not necessarily written).
    ///
    /// `None` for collections with compute frontiers equal to their write frontiers.
    pub compute_probe: Option<probe::Handle<Timestamp>>,
    /// Logging state maintained for this collection.
    logging: Option<CollectionLogging>,
    /// Metrics tracked for this collection.
    metrics: CollectionMetrics,
    /// Send-side to transition a dataflow from read-only mode to read-write mode.
    ///
    /// All dataflows start in read-only mode. Only after receiving a
    /// `AllowWrites` command from the controller will they transition to
    /// read-write mode.
    ///
    /// A dataflow in read-only mode must not affect any external state.
    ///
    /// NOTE: In the future, we might want a more complicated flag, for example
    /// something that tells us after which timestamp we are allowed to write.
    /// In this first version we are keeping things as simple as possible!
    read_only_tx: watch::Sender<bool>,
    /// Receive-side to observe whether a dataflow is in read-only mode.
    pub read_only_rx: watch::Receiver<bool>,
}

impl CollectionState {
    fn new(
        dataflow_index: Rc<usize>,
        is_subscribe_or_copy: bool,
        as_of: Antichain<Timestamp>,
        metrics: CollectionMetrics,
    ) -> Self {
        // We always initialize as read_only=true. Only when we're explicitly
        // allowed to we switch to read-write.
        let (read_only_tx, read_only_rx) = watch::channel(true);

        Self {
            reported_frontiers: ReportedFrontiers::new(),
            dataflow_index,
            is_subscribe_or_copy,
            as_of,
            sink_token: None,
            sink_write_frontier: None,
            input_probes: Default::default(),
            compute_probe: None,
            logging: None,
            metrics,
            read_only_tx,
            read_only_rx,
        }
    }

    /// Return the frontiers that have been reported to the controller.
    fn reported_frontiers(&self) -> &ReportedFrontiers {
        &self.reported_frontiers
    }

    /// Reset all reported frontiers to the given value.
    pub fn reset_reported_frontiers(&mut self, frontier: ReportedFrontier) {
        self.reported_frontiers.write_frontier = frontier.clone();
        self.reported_frontiers.input_frontier = frontier.clone();
        self.reported_frontiers.output_frontier = frontier;
    }

    /// Set the write frontier that has been reported to the controller.
    fn set_reported_write_frontier(&mut self, frontier: ReportedFrontier) {
        if let Some(logging) = &mut self.logging {
            let time = match &frontier {
                ReportedFrontier::Reported(frontier) => frontier.get(0).copied(),
                ReportedFrontier::NotReported { .. } => Some(Timestamp::MIN),
            };
            logging.set_frontier(time);
        }

        self.reported_frontiers.write_frontier = frontier;
    }

    /// Set the input frontier that has been reported to the controller.
    fn set_reported_input_frontier(&mut self, frontier: ReportedFrontier) {
        // Use this opportunity to update our input frontier logging.
        if let Some(logging) = &mut self.logging {
            for (id, probe) in &self.input_probes {
                let new_time = probe.with_frontier(|frontier| frontier.as_option().copied());
                logging.set_import_frontier(*id, new_time);
            }
        }

        self.reported_frontiers.input_frontier = frontier;
    }

    /// Set the output frontier that has been reported to the controller.
    fn set_reported_output_frontier(&mut self, frontier: ReportedFrontier) {
        let already_hydrated = self.hydrated();

        self.reported_frontiers.output_frontier = frontier;

        if !already_hydrated && self.hydrated() {
            if let Some(logging) = &mut self.logging {
                logging.set_hydrated();
            }
            self.metrics.record_collection_hydrated();
        }
    }

    /// Return whether this collection is hydrated.
    fn hydrated(&self) -> bool {
        match &self.reported_frontiers.output_frontier {
            ReportedFrontier::Reported(frontier) => PartialOrder::less_than(&self.as_of, frontier),
            ReportedFrontier::NotReported { .. } => false,
        }
    }

    /// Allow writes for this collection.
    fn allow_writes(&self) {
        info!(
            dataflow_index = *self.dataflow_index,
            export = ?self.logging.as_ref().map(|l| l.export_id()),
            "allowing writes for dataflow",
        );
        let _ = self.read_only_tx.send(false);
    }
}

/// Tests of the sweep that drives the pending index peeks.
///
/// Built over a live [`ComputeState`] and a real timely worker rather than over the budget alone,
/// because what these pin is where a peek's walk runs, which is decided by the sweep and observed
/// through the metrics a driver leaves behind.
#[cfg(test)]
mod peek_sweep_tests {
    use mz_compute_types::dyncfgs::{
        ENABLE_INDEX_PEEK_OFFLOAD, INDEX_PEEK_ACTIVATION_BUDGET, INDEX_PEEK_INLINE_BUDGET,
        PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES,
    };
    use mz_dyncfg::ConfigUpdates;
    use mz_persist_client::Schemas;
    use mz_persist_client::cache::PersistClientCache;
    use mz_secrets::InMemorySecretsController;
    use mz_storage_types::connections::ConnectionContext;
    use timely::WorkerConfig;
    use timely::communication::Allocator;
    use tokio::sync::mpsc;

    use crate::metrics::ComputeMetrics;
    use crate::server::ComputeRuntimeRole;

    use super::index_peek_tests::{
        TARGET_ID, cancelling_errors, index_peek_with_uuid, rows_answer, trace_bundle, wide_ok_rows,
    };
    use super::*;

    /// The per-peek budget the budget-arming tests configure. Distinct from both the unbounded
    /// grant and the parameter's default, so that an assertion on it says the configured value was
    /// read.
    const INLINE_BUDGET: usize = 12_345;

    /// How many keys the index the placement-policy tests peek holds.
    ///
    /// More positions than the production inline budget, so a walk of the whole index cannot
    /// finish on the worker while a walk that seeks to one key finishes there comfortably. The two
    /// halves of the policy are the same index at the same budget, differing only in what the peek
    /// asks for.
    const WIDE_INDEX_KEYS: u64 = 2_000;

    /// How many keys the index the budget tests peek holds. Small, because what those tests turn
    /// on is how many peeks got a turn rather than how far each one walked.
    const SMALL_INDEX_KEYS: u64 = 6;

    /// How many rows a walk accumulates before its batch is full, in the tests that drive a
    /// hand-back.
    ///
    /// More than the inline slice can accumulate within the production budget and fewer than the
    /// wide index holds, which is what puts the crossing after the promotion rather than before it
    /// or never.
    const HAND_BACK_AT_ROWS: u64 = 1_500;

    /// How many activations a test drives before it declares a peek stuck.
    ///
    /// A promoted walk over these traces needs one slice, and a peek passed over for want of
    /// budget needs one activation per peek ahead of it, so a sweep that makes progress finishes
    /// far inside this bound and one that does not fails rather than hanging the suite.
    const SWEEP_BOUND: usize = 200;

    /// Peek uuids, named in the order they sort, because which peek a sweep serves first turns on
    /// that order.
    const PEEK_A: Uuid = Uuid::from_u128(1);
    const PEEK_B: Uuid = Uuid::from_u128(2);
    const PEEK_C: Uuid = Uuid::from_u128(3);
    const PEEK_D: Uuid = Uuid::from_u128(4);

    /// The compute state, the timely worker, and the response channel one activation runs against,
    /// held together across activations.
    ///
    /// A promoted walk outlives the sweep that promoted it, so the worker whose activator it wakes,
    /// the responses it eventually produces, and the state it stays pending in all have to survive
    /// the call that started it.
    struct Harness {
        state: ComputeState,
        timely_worker: TimelyWorker,
        response_tx: ResponseSender,
        responses: mpsc::UnboundedReceiver<(ComputeResponse, Uuid)>,
    }

    impl Harness {
        /// A harness as `CreateInstance` leaves one, with `configure` applied to the worker
        /// configuration as `UpdateConfiguration` applies a change.
        fn new(configure: impl FnOnce(&mut ConfigUpdates)) -> Self {
            let metrics_registry = MetricsRegistry::new();
            let metrics =
                ComputeMetrics::register_with(&metrics_registry, ComputeRuntimeRole::Solo)
                    .for_worker(0);
            let context = ComputeInstanceContext {
                scratch_directory: None,
                worker_core_affinity: false,
                connection_context: ConnectionContext::for_tests(Arc::new(
                    InMemorySecretsController::new(),
                )),
            };

            let state = ComputeState::new(
                Arc::new(PersistClientCache::new_no_metrics()),
                TxnsContext::default(),
                metrics,
                Arc::new(TracingHandle::disabled()),
                context,
                metrics_registry,
                1,
                Arc::new(PeekPermits::new(1)),
                None,
            );

            // The worker applies these through `UpdateConfiguration`, which reaches the budget
            // through the handles it holds rather than through any state the command touches.
            let mut updates = ConfigUpdates::default();
            configure(&mut updates);
            updates.apply(&state.worker_config);

            // The worker is given a timer, because a worker without one reports work waiting
            // whatever its activations hold, which would make `Harness::park_for` vacuous.
            let timely_worker = TimelyWorker::new(
                WorkerConfig::default(),
                Allocator::Thread(Default::default()),
                Some(Instant::now()),
            );

            let (response_tx, responses) = mpsc::unbounded_channel();
            let mut response_tx = ResponseSender::new(response_tx, 0);
            response_tx.set_nonce(Uuid::nil());

            Self {
                state,
                timely_worker,
                response_tx,
                responses,
            }
        }

        fn active(&mut self) -> ActiveComputeState<'_> {
            ActiveComputeState {
                timely_worker: &mut self.timely_worker,
                compute_state: &mut self.state,
                response_tx: &mut self.response_tx,
            }
        }

        /// Runs one sweep over the pending peeks, as an activation of the worker does.
        fn sweep(&mut self) {
            self.active().process_peeks();
        }

        /// Makes `peek` pending over `bundle`, as a peek whose frontiers were not yet ready is
        /// left pending.
        fn add_pending(&mut self, peek: Peek, bundle: TraceBundle) {
            let pending = PendingPeek::index(peek, bundle);
            let uuid = pending.peek().uuid;
            let replaced = self.state.pending_peeks.insert(uuid, pending);
            assert!(replaced.is_none(), "each pending peek needs its own uuid");
        }

        /// Which kind of pending peek `uuid` names, or `None` when no peek is pending under it.
        ///
        /// Named rather than matched, so that a test says which driver holds the peek and a
        /// failure says which one holds it instead.
        fn pending(&self, uuid: Uuid) -> Option<&'static str> {
            self.state.pending_peeks.get(&uuid).map(|peek| match peek {
                PendingPeek::Index(_) => "index",
                PendingPeek::Persist(_) => "persist",
                PendingPeek::Stash(_) => "stash",
                PendingPeek::Offloaded(_) => "offloaded",
            })
        }

        /// The uuids of the peeks still pending, in the order the sweep takes them.
        fn pending_uuids(&self) -> Vec<Uuid> {
            self.state.pending_peeks.keys().copied().collect()
        }

        /// The peek responses sent since this was last called, in the order they were sent.
        fn peek_responses(&mut self) -> Vec<(Uuid, PeekResponse)> {
            let mut responses = Vec::new();
            while let Ok((response, _nonce)) = self.responses.try_recv() {
                match response {
                    ComputeResponse::PeekResponse(uuid, response, _otel_ctx) => {
                        responses.push((uuid, response))
                    }
                    other => panic!("a peek sweep sent {other:?}"),
                }
            }
            responses
        }

        /// The walks each substrate has driven to an outcome, as `(inline, offloaded)`.
        fn walks(&self) -> (u64, u64) {
            (
                self.state.metrics.index_peek_walks_inline.get(),
                self.state.metrics.index_peek_walks_offloaded.get(),
            )
        }

        /// How long the worker would park before its next activation, as `step_or_park` reads it.
        ///
        /// `None` is an indefinite park, which is what a worker with no dataflow and no pending
        /// activation does.
        fn park_for(&self) -> Option<Duration> {
            let activations = self.timely_worker.activations();
            let mut activations = activations.borrow_mut();
            activations.advance();
            activations.empty_for()
        }

        /// Runs activations until nothing is pending, and reports the responses they produced.
        ///
        /// Bounded, so a peek that never answers fails here rather than hanging the suite. The
        /// pause between activations is what lets a promoted walk's task run, which on a
        /// single-threaded runtime happens nowhere else.
        ///
        /// The pause is a sleep rather than a yield because a peek taken to the stash waits on an
        /// upload whose work happens off this runtime. Yielding would spin against that instead of
        /// letting it finish, which would turn the bound into a measure of how fast the machine
        /// runs the spin.
        async fn drain(&mut self) -> Vec<(Uuid, PeekResponse)> {
            let mut responses = Vec::new();
            for _ in 0..SWEEP_BOUND {
                self.sweep();
                responses.extend(self.peek_responses());
                if self.state.pending_peeks.is_empty() {
                    return responses;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            panic!("peeks were still pending after {SWEEP_BOUND} activations");
        }

        /// Runs the runtime until the two substrates have counted `walks` walks between them,
        /// without sweeping.
        ///
        /// Bounded, so a walk that never reaches an outcome fails here rather than hanging the
        /// suite. No sweep runs, so a promoted walk that finishes here leaves its outcome sitting
        /// in the channel that carries it back, which the worker has not yet read.
        async fn drive_until_walks(&self, walks: (u64, u64)) {
            for _ in 0..SWEEP_BOUND {
                if self.walks() == walks {
                    return;
                }
                tokio::task::yield_now().await;
            }
            panic!(
                "the substrates counted {:?} rather than {walks:?} within {SWEEP_BOUND} yields",
                self.walks()
            );
        }
    }

    /// A harness with the offload on and every budget at its production default, which is the
    /// configuration the placement policy is sized for.
    fn at_production_defaults() -> Harness {
        Harness::new(|updates| {
            updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
        })
    }

    /// A harness with the offload on and the per-activation aggregate set to `aggregate`.
    fn with_activation_budget(aggregate: usize) -> Harness {
        Harness::new(move |updates| {
            updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
            updates.add(&INDEX_PEEK_ACTIVATION_BUDGET, aggregate);
        })
    }

    /// The answer a peek of the whole index holding `keys` gives.
    ///
    /// Both the promoted walk and the inline walk are compared against this rather than against
    /// each other, so each test states the answer it expects instead of two runs agreeing on
    /// whatever they produced.
    fn whole_index_answer(keys: &[Row]) -> PeekResponse {
        rows_answer(keys.iter().cloned())
    }

    /// The positions one peek of the index holding `keys` spends, measured by letting a sweep
    /// spend an aggregate wide enough that nothing bounds it and reading what it charged.
    ///
    /// Measured rather than assumed, so that a test which configures the aggregate in multiples of
    /// a walk states its budget in the unit the code charges. A constant here would let a change
    /// to what a walk costs turn the test into one that asserts nothing.
    fn walk_cost(keys: &[Row]) -> usize {
        const WIDE_AGGREGATE: usize = 1_000_000;

        let mut harness = with_activation_budget(WIDE_AGGREGATE);
        harness.add_pending(
            index_peek_with_uuid(PEEK_A, None),
            trace_bundle(keys, cancelling_errors(0)),
        );
        harness.sweep();

        assert_eq!(
            harness.pending(PEEK_A),
            None,
            "the peek must answer in one slice for what it charged to be the cost of a whole walk"
        );
        let remaining =
            harness.state.peek_budget.remaining().expect(
                "the offload is on and the sweep granted a slice, so the budget is bounded",
            );
        let cost = WIDE_AGGREGATE - remaining;
        // A driver that charged the fuel it granted rather than the fuel it spent would report a
        // cost equal to the grant, and a test sizing its aggregate from that measurement would
        // then agree with itself at the wrong number. A walk visits one position per key plus what
        // the error walk spends reaching the end of an empty trace, which is nothing like the
        // per-peek budget the walk was granted.
        assert!(
            (keys.len()..*INDEX_PEEK_INLINE_BUDGET.default()).contains(&cost),
            "a walk of {} keys charged {cost} positions, which is not what such a walk visits",
            keys.len()
        );
        cost
    }

    /// A peek that arrives before any sweep has run is granted a bounded slice, so a walk that
    /// outruns it leaves the worker.
    ///
    /// The commands a worker has queued are drained in full before it sweeps its pending peeks,
    /// and `handle_peek` runs a peek's first slice as the peek arrives. On a fresh replica that
    /// drain is the controller's whole history: the `CreateInstance` that builds this state, the
    /// `UpdateConfiguration` that turns the offload on, and then every peek the controller
    /// re-sends. A budget armed only where an activation begins would grant each of those peeks
    /// unbounded fuel, and unbounded fuel never suspends, so each would walk to its answer on a
    /// worker that is hydrating and holding the largest backlog it will ever hold.
    #[mz_ore::test(tokio::test)]
    async fn a_peek_arriving_before_any_sweep_is_granted_a_bounded_slice() {
        let keys = wide_ok_rows(WIDE_INDEX_KEYS);

        let mut harness = at_production_defaults();
        harness
            .state
            .traces
            .set(TARGET_ID, trace_bundle(&keys, cancelling_errors(0)));

        harness
            .active()
            .handle_peek(index_peek_with_uuid(PEEK_A, None));

        assert_eq!(
            harness.pending(PEEK_A),
            Some("offloaded"),
            "a peek arriving before any sweep outran a bounded slice and was promoted"
        );
        assert_eq!(
            harness.drain().await,
            vec![(PEEK_A, whole_index_answer(&keys))]
        );
        assert_eq!(
            harness.walks(),
            (0, 1),
            "the promoted driver ended the walk, so no slice of it ran on the worker"
        );
    }

    /// An activation that finds nothing pending still begins one, which is what refills the
    /// aggregate the peeks arriving before the next sweep are granted out of.
    ///
    /// A replica whose index peeks all answer inline leaves none of them pending, so no sweep of
    /// its ever visits a peek. Were an activation begun only where the sweep finds work, the
    /// aggregate would drain across the arrivals such a replica serves and then pass every later
    /// arrival over, deferring peeks the worker had the budget to answer.
    #[mz_ore::test(tokio::test)]
    async fn an_idle_activation_refills_the_aggregate() {
        let mut harness = Harness::new(|updates| {
            updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
            updates.add(&INDEX_PEEK_INLINE_BUDGET, INLINE_BUDGET);
        });
        assert!(harness.state.pending_peeks.is_empty());

        // Spend the aggregate, as the peeks an activation serves do.
        assert_eq!(harness.state.peek_budget.grant(), Some(INLINE_BUDGET));
        harness.state.peek_budget.charge(usize::MAX);
        assert_eq!(harness.state.peek_budget.grant(), None);

        harness.sweep();

        assert_eq!(harness.state.peek_budget.grant(), Some(INLINE_BUDGET));
    }

    /// An activation that finds nothing pending drops the resume point, which nothing else
    /// consumes once the peek it names is gone.
    ///
    /// Cancellation removes a deferred peek from the map, and `reconcile` empties the map
    /// wholesale, both without touching the resume point. Left set on a replica whose peeks all
    /// answer inline, it would make `handle_peek` ask for an extra worker iteration for every peek
    /// the replica ever serves.
    #[mz_ore::test(tokio::test)]
    async fn an_idle_activation_drops_the_resume_point() {
        let mut harness = Harness::new(|updates| {
            updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
            updates.add(&INDEX_PEEK_INLINE_BUDGET, INLINE_BUDGET);
        });
        harness.state.peek_resume_at = Some(PEEK_A);
        assert!(harness.state.pending_peeks.is_empty());

        harness.sweep();

        assert_eq!(harness.state.peek_resume_at, None);
    }

    /// A peek whose walk outruns the production inline budget leaves the worker, and the driver
    /// that finished the walk counts it as offloaded.
    ///
    /// This is what says a peek is promoted at all. The rows a promoted walk answers with are the
    /// rows an inline walk answers with, so a suite comparing only answers would pass with the
    /// whole mechanism inert.
    #[mz_ore::test(tokio::test)]
    async fn a_scan_that_outruns_the_production_budget_is_promoted() {
        let keys = wide_ok_rows(WIDE_INDEX_KEYS);
        assert!(
            u64::cast_from(*INDEX_PEEK_INLINE_BUDGET.default()) < WIDE_INDEX_KEYS,
            "the index must hold more positions than the production budget lets a peek walk inline"
        );

        let mut harness = at_production_defaults();
        harness.add_pending(
            index_peek_with_uuid(PEEK_A, None),
            trace_bundle(&keys, cancelling_errors(0)),
        );

        harness.sweep();

        assert_eq!(
            harness.pending(PEEK_A),
            Some("offloaded"),
            "a walk that outran its inline budget belongs to the promoted driver"
        );
        assert_eq!(
            harness.walks(),
            (0, 0),
            "promotion is not a terminal outcome, so neither driver has counted the walk yet"
        );

        assert_eq!(
            harness.drain().await,
            vec![(PEEK_A, whole_index_answer(&keys))]
        );
        assert_eq!(
            harness.walks(),
            (0, 1),
            "the promoted driver ended the walk and counted it"
        );
    }

    /// A point lookup at the production inline budget is answered on the worker, over the very
    /// index whose full walk is promoted.
    ///
    /// This is the half of the placement policy that a layer promoting everything would still
    /// pass every equivalence test while destroying. The budget is sized for point lookups and
    /// nothing else, and what makes that true is measured here rather than argued.
    #[mz_ore::test(tokio::test)]
    async fn a_point_lookup_at_the_production_budget_stays_inline() {
        let keys = wide_ok_rows(WIDE_INDEX_KEYS);
        let literal = keys[7].clone();

        let mut harness = at_production_defaults();
        harness.add_pending(
            index_peek_with_uuid(PEEK_A, Some(vec![literal.clone()])),
            trace_bundle(&keys, cancelling_errors(0)),
        );

        harness.sweep();

        assert_eq!(
            harness.peek_responses(),
            vec![(PEEK_A, rows_answer([literal]))]
        );
        assert_eq!(
            harness.pending(PEEK_A),
            None,
            "a peek answered inline leaves nothing pending"
        );
        assert_eq!(
            harness.walks(),
            (1, 0),
            "a point lookup finishes inside the inline budget and is never promoted"
        );
    }

    /// With the kill switch off, the peek that the production budget promotes instead walks to its
    /// answer on the worker, and that answer is the one the promoted walk gives.
    ///
    /// Both this and the promoted run are compared against [`whole_index_answer`], so the
    /// equivalence promotion owes is stated rather than inferred from two runs agreeing.
    #[mz_ore::test(tokio::test)]
    async fn the_kill_switch_answers_the_same_scan_inline() {
        let keys = wide_ok_rows(WIDE_INDEX_KEYS);

        // No configuration at all, which is the kill switch in the position production ships it.
        let mut harness = Harness::new(|_updates| ());
        harness.add_pending(
            index_peek_with_uuid(PEEK_A, None),
            trace_bundle(&keys, cancelling_errors(0)),
        );

        harness.sweep();

        assert_eq!(
            harness.peek_responses(),
            vec![(PEEK_A, whole_index_answer(&keys))]
        );
        assert_eq!(
            harness.pending(PEEK_A),
            None,
            "an unbounded slice walks to the answer without suspending"
        );
        assert_eq!(
            harness.walks(),
            (1, 0),
            "the kill switch promotes nothing, however far a peek walks"
        );
    }

    /// One activation serves what the per-activation aggregate allows and passes the rest over,
    /// however many peeks are pending.
    ///
    /// The sweep visits every pending peek, so a per-peek budget with no aggregate would let a
    /// burst of N peeks cost N inline budgets in one pass, unbounded in N.
    #[mz_ore::test(tokio::test)]
    async fn the_activation_budget_bounds_a_burst() {
        let keys = wide_ok_rows(SMALL_INDEX_KEYS);

        let mut harness = with_activation_budget(1);
        for uuid in [PEEK_A, PEEK_B, PEEK_C, PEEK_D] {
            harness.add_pending(
                index_peek_with_uuid(uuid, None),
                trace_bundle(&keys, cancelling_errors(0)),
            );
        }

        harness.sweep();

        assert_eq!(
            harness.peek_responses(),
            vec![(PEEK_A, whole_index_answer(&keys))],
            "an aggregate of one position serves one peek per activation"
        );
        assert_eq!(
            harness.pending_uuids(),
            vec![PEEK_B, PEEK_C, PEEK_D],
            "the peeks that got no turn are left untouched"
        );
        assert_eq!(harness.state.peek_resume_at, Some(PEEK_B));

        // The burst drains rather than wedging, one peek per activation.
        assert_eq!(
            harness.drain().await,
            vec![
                (PEEK_B, whole_index_answer(&keys)),
                (PEEK_C, whole_index_answer(&keys)),
                (PEEK_D, whole_index_answer(&keys)),
            ]
        );
    }

    /// The aggregate is charged what a slice walked rather than what it was granted, so an
    /// activation serves as many peeks as their walks fit into.
    ///
    /// Charging the grant instead would spend the whole aggregate on the first peek whatever it
    /// walked, which is the difference between an activation serving a burst of point lookups and
    /// serving one of them. Comparing this run against an unbudgeted one would not catch it, since
    /// both would answer the same rows. The assertion is against the budget this test supplied.
    #[mz_ore::test(tokio::test)]
    async fn the_aggregate_is_charged_what_the_slices_walked() {
        let keys = wide_ok_rows(SMALL_INDEX_KEYS);
        let cost = walk_cost(&keys);

        // Room for two walks of this index and no more. The per-peek budget is left at its
        // production default, which is far above one walk, so nothing here is promoted and what
        // decides how many peeks got a turn is the aggregate alone.
        let mut harness = with_activation_budget(2 * cost);
        for uuid in [PEEK_A, PEEK_B, PEEK_C, PEEK_D] {
            harness.add_pending(
                index_peek_with_uuid(uuid, None),
                trace_bundle(&keys, cancelling_errors(0)),
            );
        }

        harness.sweep();

        assert_eq!(
            harness.peek_responses(),
            vec![
                (PEEK_A, whole_index_answer(&keys)),
                (PEEK_B, whole_index_answer(&keys)),
            ],
            "an aggregate of two walks serves two peeks and passes the rest over"
        );
        assert_eq!(harness.pending_uuids(), vec![PEEK_C, PEEK_D]);
        assert_eq!(harness.walks(), (2, 0), "neither served peek was promoted");
    }

    /// A peek passed over for want of budget is served before a peek that arrived after it, even
    /// one whose uuid sorts ahead of it.
    ///
    /// The pending peeks are a map keyed by uuid, so a sweep taking them in map order alone would
    /// let a newly arrived peek take the turn of one that has already waited an activation, and it
    /// would do so again on every activation.
    #[mz_ore::test(tokio::test)]
    async fn a_passed_over_peek_is_served_before_one_that_arrived_later() {
        let keys = wide_ok_rows(SMALL_INDEX_KEYS);
        let answer = whole_index_answer(&keys);

        let mut harness = with_activation_budget(1);
        for uuid in [PEEK_B, PEEK_C] {
            harness.add_pending(
                index_peek_with_uuid(uuid, None),
                trace_bundle(&keys, cancelling_errors(0)),
            );
        }

        harness.sweep();

        assert_eq!(harness.peek_responses(), vec![(PEEK_B, answer.clone())]);
        assert_eq!(harness.state.peek_resume_at, Some(PEEK_C));

        // A peek arrives whose uuid sorts ahead of the one that was passed over.
        harness.add_pending(
            index_peek_with_uuid(PEEK_A, None),
            trace_bundle(&keys, cancelling_errors(0)),
        );

        harness.sweep();

        assert_eq!(
            harness.peek_responses(),
            vec![(PEEK_C, answer)],
            "the peek that waited an activation takes the next turn"
        );
    }

    /// A resume point naming a peek that has since been cancelled still names where the next sweep
    /// starts.
    ///
    /// The peek a caller cancels is disproportionately one that was passed over for want of
    /// budget, because that is the peek that has been waiting, and cancellation removes it from
    /// the map without touching the resume point. What the resume point names is a position in the
    /// uuid ordering rather than a peek, so the sweep resumes at the first surviving peek that
    /// sorts at or after it and the peeks that were served ahead of it still go last.
    #[mz_ore::test(tokio::test)]
    async fn a_resume_point_outliving_its_peek_still_names_where_to_resume() {
        let keys = wide_ok_rows(SMALL_INDEX_KEYS);
        let answer = whole_index_answer(&keys);

        let mut harness = with_activation_budget(1);
        for uuid in [PEEK_B, PEEK_C] {
            harness.add_pending(
                index_peek_with_uuid(uuid, None),
                trace_bundle(&keys, cancelling_errors(0)),
            );
        }

        harness.sweep();

        assert_eq!(harness.peek_responses(), vec![(PEEK_B, answer.clone())]);
        assert_eq!(harness.state.peek_resume_at, Some(PEEK_C));

        harness.active().handle_cancel_peek(PEEK_C);

        assert_eq!(
            harness.peek_responses(),
            vec![(PEEK_C, PeekResponse::Canceled)]
        );
        assert_eq!(
            harness.state.peek_resume_at,
            Some(PEEK_C),
            "cancelling a peek leaves the resume point naming it"
        );

        // Peeks arrive on both sides of the cancelled one in the uuid ordering.
        for uuid in [PEEK_A, PEEK_D] {
            harness.add_pending(
                index_peek_with_uuid(uuid, None),
                trace_bundle(&keys, cancelling_errors(0)),
            );
        }

        harness.sweep();

        assert_eq!(
            harness.peek_responses(),
            vec![(PEEK_D, answer)],
            "the sweep resumes at the first peek sorting after the cancelled one"
        );
        assert_eq!(harness.state.peek_resume_at, Some(PEEK_A));
    }

    /// A resume point sorting past every pending peek starts the next sweep at the first of them,
    /// which is where a ring wraps to.
    ///
    /// This is the end of the ordering, where the rotation is by the whole length of it. Rotating
    /// a sweep's peeks by their own count leaves them where they were, and where they were is the
    /// wrap the ring owes the peeks that sort ahead of the resume point.
    #[mz_ore::test(tokio::test)]
    async fn a_resume_point_past_every_pending_peek_wraps_to_the_first() {
        let keys = wide_ok_rows(SMALL_INDEX_KEYS);
        let answer = whole_index_answer(&keys);

        let mut harness = with_activation_budget(1);
        for uuid in [PEEK_C, PEEK_D] {
            harness.add_pending(
                index_peek_with_uuid(uuid, None),
                trace_bundle(&keys, cancelling_errors(0)),
            );
        }

        harness.sweep();

        assert_eq!(harness.peek_responses(), vec![(PEEK_C, answer.clone())]);
        assert_eq!(harness.state.peek_resume_at, Some(PEEK_D));

        harness.active().handle_cancel_peek(PEEK_D);
        assert_eq!(
            harness.peek_responses(),
            vec![(PEEK_D, PeekResponse::Canceled)]
        );

        // Every peek that survives the cancellation sorts ahead of the resume point.
        for uuid in [PEEK_A, PEEK_B] {
            harness.add_pending(
                index_peek_with_uuid(uuid, None),
                trace_bundle(&keys, cancelling_errors(0)),
            );
        }

        harness.sweep();

        assert_eq!(
            harness.peek_responses(),
            vec![(PEEK_A, answer)],
            "a resume point past every pending peek wraps to the first of them"
        );
        assert_eq!(harness.state.peek_resume_at, Some(PEEK_B));
    }

    /// A peek deferred as it arrives leaves an activation behind, so a worker with nothing else to
    /// do does not park on it.
    ///
    /// `handle_peek` runs a peek's first slice as the peek arrives, and a peek that finds the
    /// activation's budget spent is left pending there with no sweep to follow. `run_client` parks
    /// after its maintenance tick, indefinitely at a zero maintenance interval, and nothing else
    /// would wake it: the peeks that spent the budget were answered or promoted, and neither
    /// leaves an activation behind.
    #[mz_ore::test(tokio::test)]
    async fn a_peek_deferred_as_it_arrives_leaves_an_activation_behind() {
        let keys = wide_ok_rows(SMALL_INDEX_KEYS);

        let mut harness = with_activation_budget(1);
        harness
            .state
            .traces
            .set(TARGET_ID, trace_bundle(&keys, cancelling_errors(0)));

        // One peek spends this activation's whole aggregate.
        harness.add_pending(
            index_peek_with_uuid(PEEK_A, None),
            trace_bundle(&keys, cancelling_errors(0)),
        );
        harness.sweep();
        assert_eq!(
            harness.pending(PEEK_A),
            None,
            "the first peek answered and spent the aggregate"
        );
        assert_eq!(
            harness.park_for(),
            None,
            "with nothing deferred the worker has nothing to wake for"
        );

        harness
            .active()
            .handle_peek(index_peek_with_uuid(PEEK_B, None));

        assert_eq!(
            harness.state.peek_resume_at,
            Some(PEEK_B),
            "the arriving peek found no budget and was deferred"
        );
        assert_eq!(
            harness.park_for(),
            Some(Duration::ZERO),
            "a peek deferred outside a sweep leaves the worker something to wake for"
        );
    }

    /// Cancelling a promoted peek answers it once, as cancelled, and no later activation answers
    /// it again.
    ///
    /// This is the worker's half of a cancellation. The entry the cancellation removes owns the
    /// handle to the walk, so removing it aborts the walk, and what the activations that follow
    /// have to produce is nothing at all: no second response, and no count on either substrate for
    /// a walk that never reached an outcome. The cancellation lands before the promoted task has
    /// been polled, because nothing here awaits between the sweep that promoted it and the
    /// cancellation. What a walk that was already running does with a cancellation is pinned by
    /// `peek_offload::tests::a_walk_cancelled_while_running_reports_no_outcome`, and what one that
    /// had already reached an outcome does by
    /// [`a_walk_cancelled_with_its_outcome_in_flight_is_counted`].
    #[mz_ore::test(tokio::test)]
    async fn a_cancelled_promoted_peek_is_answered_once() {
        let keys = wide_ok_rows(WIDE_INDEX_KEYS);

        let mut harness = at_production_defaults();
        harness.add_pending(
            index_peek_with_uuid(PEEK_A, None),
            trace_bundle(&keys, cancelling_errors(0)),
        );

        harness.sweep();
        assert_eq!(harness.pending(PEEK_A), Some("offloaded"));

        harness.active().handle_cancel_peek(PEEK_A);

        assert_eq!(
            harness.peek_responses(),
            vec![(PEEK_A, PeekResponse::Canceled)]
        );
        assert_eq!(harness.pending(PEEK_A), None);

        for _ in 0..SWEEP_BOUND {
            tokio::task::yield_now().await;
            harness.sweep();
        }
        assert_eq!(
            harness.peek_responses(),
            vec![],
            "a cancelled peek is answered once and never again"
        );
        assert_eq!(
            harness.walks(),
            (0, 0),
            "a cancelled walk reaches no outcome and counts on neither substrate"
        );
    }

    /// Cancelling a promoted peek whose walk has already reached its outcome answers it as
    /// cancelled and counts the walk as offloaded.
    ///
    /// This is the case the walk counters are documented with: a walk is counted where it reached
    /// an outcome, and a cancellation that lands after that point does not take the count away.
    /// The window is the one between the task sending its outcome and the worker's next sweep
    /// reading it, which is as long as the worker takes to come back around, so a replica under
    /// load spends real time in it. The counters would otherwise have to be read as a lower bound
    /// on the walks each substrate finished rather than as the count of them.
    #[mz_ore::test(tokio::test)]
    async fn a_walk_cancelled_with_its_outcome_in_flight_is_counted() {
        let keys = wide_ok_rows(WIDE_INDEX_KEYS);

        let mut harness = at_production_defaults();
        harness.add_pending(
            index_peek_with_uuid(PEEK_A, None),
            trace_bundle(&keys, cancelling_errors(0)),
        );

        harness.sweep();
        assert_eq!(harness.pending(PEEK_A), Some("offloaded"));

        // The walk runs to its outcome while no sweep collects it, which leaves the outcome in
        // flight between the task and the worker.
        harness.drive_until_walks((0, 1)).await;
        assert_eq!(
            harness.peek_responses(),
            vec![],
            "no sweep has read the outcome, so the peek is unanswered"
        );

        harness.active().handle_cancel_peek(PEEK_A);

        assert_eq!(
            harness.peek_responses(),
            vec![(PEEK_A, PeekResponse::Canceled)],
            "the cancellation answers the peek rather than the outcome in flight"
        );
        assert_eq!(
            harness.walks(),
            (0, 1),
            "a walk that reached its outcome stays counted on the substrate that ended it"
        );

        harness.sweep();
        assert_eq!(
            harness.peek_responses(),
            vec![],
            "the outcome in flight is dropped with the peek rather than answered after it"
        );
    }

    /// A harness whose peek of the whole wide index is promoted and whose promoted walk then
    /// crosses the stash threshold, swept once so that the peek is already promoted.
    ///
    /// `location` is the replica's peek stash location, which has to be present here whatever the
    /// hand-back is meant to find: a replica without one makes no scan stash-eligible, so its
    /// walks never fill a batch and never hand back at all.
    fn promoted_walk_that_hands_back(keys: &[Row], location: PersistLocation) -> Harness {
        assert!(
            u64::cast_from(*INDEX_PEEK_INLINE_BUDGET.default()) < HAND_BACK_AT_ROWS
                && HAND_BACK_AT_ROWS < WIDE_INDEX_KEYS,
            "the walk must cross the stash threshold after it is promoted and before it ends"
        );
        // The threshold is a size rather than a count, because the size of what a scan has
        // accumulated is what it compares against.
        let row_size = keys[0].byte_len() + size_of::<NonZeroUsize>();
        let threshold = usize::cast_from(HAND_BACK_AT_ROWS) * row_size;

        let mut harness = Harness::new(move |updates| {
            updates.add(&ENABLE_INDEX_PEEK_OFFLOAD, true);
            updates.add(&ENABLE_PEEK_RESPONSE_STASH, true);
            updates.add(&PEEK_RESPONSE_STASH_THRESHOLD_BYTES, threshold);
        });
        harness.state.peek_stash_persist_location = Some(location);
        harness.add_pending(
            index_peek_with_uuid(PEEK_A, None),
            trace_bundle(keys, cancelling_errors(0)),
        );

        harness.sweep();
        assert_eq!(
            harness.pending(PEEK_A),
            Some("offloaded"),
            "the walk must leave the worker before it crosses the threshold"
        );
        harness
    }

    /// Runs activations until the promoted walk of `PEEK_A` has handed back.
    ///
    /// Bounded, so a walk that never hands back fails here rather than hanging the suite. The
    /// yield between activations is what lets the promoted task run.
    async fn sweep_until_handed_back(harness: &mut Harness) {
        for _ in 0..SWEEP_BOUND {
            if harness.pending(PEEK_A) != Some("offloaded") {
                return;
            }
            tokio::task::yield_now().await;
            harness.sweep();
        }
        panic!("the promoted walk had not handed back after {SWEEP_BOUND} activations");
    }

    /// The rows a stashed peek response holds, read back out of `location` the way the coordinator
    /// reads one, in [`Row`] order.
    ///
    /// A stashed response names a persist batch rather than carrying the answer, so what the peek
    /// owes its caller is only visible from the batch. The batch is ordered as persist consolidates
    /// it rather than as the peek would have answered, and the coordinator orders what it reads
    /// back, so the rows are sorted here and compared as the set they are.
    async fn stashed_rows(
        harness: &Harness,
        location: &PersistLocation,
        response: PeekResponse,
    ) -> Vec<Row> {
        let PeekResponse::Stashed(stashed) = response else {
            panic!("a peek taken to the stash answers with a stashed response, not {response:?}");
        };

        // Opened out of the harness's own cache, because two `PersistLocation`s naming the same
        // in-memory URI reach the same blob only through the cache that opened them.
        let mut client = harness
            .state
            .persist_clients
            .open(location.clone())
            .await
            .expect("the in-memory location opens");

        let shard_id = stashed.shard_id;
        let batches = stashed
            .batches
            .into_iter()
            .map(|batch| client.batch_from_transmittable_batch(&shard_id, batch))
            .collect();
        let read_schemas: Schemas<SourceData, ()> = Schemas {
            id: None,
            key: Arc::new(stashed.relation_desc),
            val: Arc::new(UnitSchema),
        };
        let mut cursor = client
            .read_batches_consolidated::<_, _, _, i64>(
                shard_id,
                Antichain::from_elem(Timestamp::default()),
                read_schemas,
                batches,
                |_stats| true,
                *PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES.default(),
            )
            .await
            .expect("the batches are readable at the timestamp they were written at");

        let mut rows = Vec::new();
        while let Some(updates) = cursor.next().await {
            for ((key, _val), _time, diff) in updates {
                assert_eq!(diff, 1, "the index holds each key once");
                rows.push(key.0.expect("the peek stash holds no errors"));
            }
        }
        rows.sort();

        // Deleted as the coordinator deletes them once it has read them. A batch dropped without
        // this leaves its blob keys behind and says so in a warning.
        for batch in cursor.into_lease() {
            batch.delete().await;
        }
        rows
    }

    /// A promoted walk that hands back with a stash location present takes the peek to the stash,
    /// which answers it with the rows the peek would have answered with inline.
    ///
    /// This is the arm every hand-back in production takes, because a replica's stash location is
    /// set once at instance creation and nothing clears it. The peek has to become pending on the
    /// stash rather than be answered where the hand-back arrives: the rows are produced by a
    /// second walk that the worker pumps over the activations that follow, and answering here
    /// would drop them.
    #[mz_ore::test(tokio::test)]
    async fn a_promoted_hand_back_takes_the_peek_to_the_stash() {
        // The wide keys carry the `UInt64` the peek's result description declares, which is the
        // schema the stash writes its batch under. The narrow fixture rows do not.
        let keys = wide_ok_rows(WIDE_INDEX_KEYS);
        let location = PersistLocation::new_in_mem();
        let mut harness = promoted_walk_that_hands_back(&keys, location.clone());

        sweep_until_handed_back(&mut harness).await;

        assert_eq!(
            harness.pending(PEEK_A),
            Some("stash"),
            "the hand-back starts a stash upload and leaves the peek waiting on it"
        );
        assert_eq!(
            harness.peek_responses(),
            vec![],
            "a peek handed to the stash is not answered where the hand-back arrives"
        );
        assert_eq!(
            harness.walks(),
            (0, 1),
            "a hand-back is a terminal outcome of the promoted walk"
        );

        let mut responses = harness.drain().await;
        assert_eq!(responses.len(), 1, "the stashed peek answers once");
        let (uuid, response) = responses.pop().expect("length checked");
        assert_eq!(uuid, PEEK_A);
        assert_eq!(
            stashed_rows(&harness, &location, response).await,
            keys,
            "the stash holds the rows the peek would have answered with inline"
        );
    }

    /// A promoted walk that hands back with nowhere to write the rows answers the peek with an
    /// error rather than leaving it pending on a walk that has stopped.
    ///
    /// This arm is defensive only. Reaching it takes a replica that loses its stash location
    /// between the promotion and the hand-back, which nothing does, and the location is cleared
    /// here to stand in for that: `handle_create_instance` sets it and nothing clears it, while a
    /// replica that never had one makes no scan stash-eligible, so none of its walks fills a batch
    /// and hands back in the first place. The arm production takes is
    /// [`a_promoted_hand_back_takes_the_peek_to_the_stash`]'s.
    #[mz_ore::test(tokio::test)]
    async fn a_promoted_hand_back_answers_when_there_is_nowhere_to_write() {
        let keys = wide_ok_rows(WIDE_INDEX_KEYS);
        let mut harness = promoted_walk_that_hands_back(&keys, PersistLocation::new_in_mem());

        harness.state.peek_stash_persist_location = None;

        assert_eq!(
            harness.drain().await,
            vec![(
                PEEK_A,
                PeekResponse::Error(PeekError::unstructured(
                    "peek result is too large to answer inline and this replica has no peek stash \
                     location"
                ))
            )]
        );
        assert_eq!(
            harness.walks(),
            (0, 1),
            "a hand-back is a terminal outcome of the promoted walk"
        );
    }
}

/// Tests of the inline index-peek driver, and the fixtures a peek scan is tested over.
///
/// The fixtures are shared with [`peek_scan`]'s own tests, which build a scan from the same
/// [`TraceBundle`] this driver hands one.
#[cfg(test)]
pub(crate) mod index_peek_tests {
    use differential_dataflow::operators::arrange::TraceAgent;
    use differential_dataflow::trace::{Batcher, Builder, Trace};
    use mz_expr::RowSetFinishing;
    use mz_repr::{Datum, Diff, RelationDesc, SqlScalarType};
    use mz_row_spine::{RowRowBatcher, RowRowBuilder, RowRowSpine};
    use mz_timely_util::columnation::ColumnationStack;
    use timely::container::PushInto;
    use timely::dataflow::operators::generic::OperatorInfo;

    use crate::metrics::ComputeMetrics;
    use crate::server::ComputeRuntimeRole;
    use crate::typedefs::{ErrAgent, ErrSpine, RowRowAgent};

    use super::error_scan::tests::{
        ErrorUpdates, PEEK_TIMESTAMP, cancelling, error, error_batch, holding,
    };
    use super::*;

    /// The collection the peeks in these tests read.
    pub(crate) const TARGET_ID: GlobalId = GlobalId::User(1);

    /// A trace agent over a trace holding exactly `batch`.
    ///
    /// The writer is dropped here, which seals the trace to the empty frontier. That is what lets
    /// `TraceReader::cursor` hand out a cursor covering every batch, which is the only way a peek
    /// reads a trace.
    fn agent<Tr>(batch: Tr::Batch) -> TraceAgent<Tr>
    where
        Tr: Trace<Time = Timestamp> + 'static,
    {
        let info = OperatorInfo::new(0, 0, [].into());
        let (agent, mut writer) = TraceAgent::new(Tr::new(info.clone(), None, None), info, None);
        writer.insert(batch, Some(Timestamp::MIN));
        agent
    }

    /// A row holding `value` as its only datum, as the ok trace's keys hold it.
    pub(crate) fn ok_row(value: u8) -> Row {
        Row::pack_slice(&[Datum::UInt8(value)])
    }

    /// A finishing that asks for every row, in the order the walk produced them.
    pub(crate) fn trivial_finishing() -> RowSetFinishing {
        RowSetFinishing::trivial(1)
    }

    /// The ok trace of an index holding one update per key in `keys`, all at [`Timestamp::MIN`] so
    /// that every one of them is visible at [`PEEK_TIMESTAMP`]. The values are empty.
    fn oks_trace(keys: &[Row]) -> RowRowAgent<Timestamp, Diff> {
        let mut chunk = ColumnationStack::with_capacity(keys.len());
        for key in keys {
            chunk.push_into(((key.clone(), Row::default()), Timestamp::MIN, Diff::ONE));
        }
        let mut batcher = RowRowBatcher::<Timestamp, Diff>::new(None, 0);
        batcher.push_into(chunk);
        let (mut chain, description) = batcher.seal(Antichain::from_elem(Timestamp::MAX));
        let batch = RowRowBuilder::<Timestamp, Diff>::seal(&mut chain, description);
        agent::<RowRowSpine<Timestamp, Diff>>(batch)
    }

    /// The error trace of an index holding `updates`.
    fn errs_trace(updates: ErrorUpdates) -> ErrAgent<Timestamp, Diff> {
        agent::<ErrSpine<Timestamp, Diff>>(error_batch(updates))
    }

    /// The traces of an index whose ok side holds `keys` and whose error side holds `errors`.
    pub(crate) fn trace_bundle(keys: &[Row], errors: ErrorUpdates) -> TraceBundle {
        TraceBundle::new(oks_trace(keys), errs_trace(errors))
    }

    /// Errors that a walk over them examines one by one and finds none of, because each cancels to
    /// zero at [`PEEK_TIMESTAMP`].
    pub(crate) fn cancelling_errors(count: usize) -> ErrorUpdates {
        let errors: Vec<_> = (0..count).map(error).collect();
        errors.iter().flat_map(cancelling).collect()
    }

    /// `count` errors that cancel to zero at [`PEEK_TIMESTAMP`] plus one that does not, and the
    /// answer a walk that reaches the latter gives.
    ///
    /// The serialized order of an error does not follow its index, so the errors are sorted and
    /// the answering one taken from the end. A walk that visits keys in order therefore reaches it
    /// last, having examined all `count` of the others.
    pub(crate) fn answering_errors(count: usize) -> (ErrorUpdates, PeekError) {
        let mut errors: Vec<_> = (0..count + 1).map(error).collect();
        errors.sort();
        let (answering, cancelled) = errors.split_last().expect("non-empty");
        let mut updates: ErrorUpdates = cancelled.iter().flat_map(cancelling).collect();
        updates.extend(holding(answering));
        (updates, PeekError::from(answering.deserialize()))
    }

    /// A peek of [`TARGET_ID`] at [`PEEK_TIMESTAMP`].
    ///
    /// The projection drops every column but the first, so a peek with a literal constraint and
    /// one without return rows of the same shape: the cursor's key is one datum, the values are
    /// empty, and a literal constraint contributes a second datum that a join in a dataflow would
    /// have added.
    ///
    /// The result description carries that one column, because its arity is what decides whether
    /// the peek's finishing streams, and a finishing that streams is what makes a peek eligible
    /// for the peek stash.
    pub(crate) fn index_peek(
        finishing: RowSetFinishing,
        literal_constraints: Option<Vec<Row>>,
    ) -> Peek {
        let arity = if literal_constraints.is_some() { 2 } else { 1 };
        let map_filter_project = mz_expr::MapFilterProject::new(arity)
            .project([0])
            .into_plan()
            .expect("valid plan")
            .into_nontemporal()
            .expect("non-temporal plan");
        let result_desc = RelationDesc::builder()
            .with_column("value", SqlScalarType::UInt64.nullable(false))
            .finish();
        Peek {
            target: PeekTarget::Index { id: TARGET_ID },
            result_desc,
            literal_constraints,
            uuid: Uuid::nil(),
            timestamp: PEEK_TIMESTAMP,
            finishing,
            map_filter_project,
            otel_ctx: OpenTelemetryContext::empty(),
        }
    }

    /// A peek of [`TARGET_ID`] carrying `uuid`, so that a test with several pending peeks can say
    /// which one a response answered.
    pub(crate) fn index_peek_with_uuid(uuid: Uuid, literal_constraints: Option<Vec<Row>>) -> Peek {
        let mut peek = index_peek(trivial_finishing(), literal_constraints);
        peek.uuid = uuid;
        peek
    }

    /// The keys of an index holding `count` distinct rows, in the order the ok walk visits them.
    ///
    /// The datum is wider than [`ok_row`]'s so that an index can be built with more positions than
    /// the production inline budget allows a peek to walk on the worker. Sorted here rather than
    /// assumed, because the trace holds its keys in [`Row`] order and the answer a full walk gives
    /// is that order.
    pub(crate) fn wide_ok_rows(count: u64) -> Vec<Row> {
        let mut keys: Vec<Row> = (0..count)
            .map(|value| Row::pack_slice(&[Datum::UInt64(value)]))
            .collect();
        keys.sort();
        keys
    }

    /// The metrics an index peek observes into, registered into a registry the test owns so it
    /// can read them back.
    struct TestMetrics {
        metrics: WorkerMetrics,
        walk: PeekWalkMetrics,
    }

    impl TestMetrics {
        fn new() -> Self {
            let metrics =
                ComputeMetrics::register_with(&MetricsRegistry::new(), ComputeRuntimeRole::Solo)
                    .for_worker(0);
            let walk = PeekWalkMetrics::new(&metrics);
            Self { metrics, walk }
        }

        fn as_metrics(&self) -> IndexPeekMetrics<'_> {
            IndexPeekMetrics {
                seek_fulfillment_seconds: &self.metrics.index_peek_seek_fulfillment_seconds,
                frontier_check_seconds: &self.metrics.index_peek_frontier_check_seconds,
                walk: &self.walk,
            }
        }

        /// How often each metric that `collect_finished_data` can observe into was observed.
        ///
        /// The two histograms the enclosing `seek_fulfillment` owns are left out, because the
        /// tests that read this call `collect_finished_data` directly.
        fn observations(&self) -> BTreeMap<&'static str, u64> {
            let metrics = &self.metrics;
            BTreeMap::from([
                ("walks_inline", metrics.index_peek_walks_inline.get()),
                ("walks_offloaded", metrics.index_peek_walks_offloaded.get()),
                (
                    "error_scan_seconds",
                    metrics.index_peek_error_scan_seconds.get_sample_count(),
                ),
                (
                    "cursor_setup_seconds",
                    metrics.index_peek_cursor_setup_seconds.get_sample_count(),
                ),
                (
                    "row_iteration_seconds",
                    metrics.index_peek_row_iteration_seconds.get_sample_count(),
                ),
                (
                    "row_iteration_rows",
                    metrics.index_peek_row_iteration_rows.get_sample_count(),
                ),
                (
                    "result_sort_seconds",
                    metrics.index_peek_result_sort_seconds.get_sample_count(),
                ),
                (
                    "result_sort_rows",
                    metrics.index_peek_result_sort_rows.get_sample_count(),
                ),
                (
                    "row_collection_seconds",
                    metrics.index_peek_row_collection_seconds.get_sample_count(),
                ),
            ])
        }
    }

    /// The fuel the inline driver spends with the offload off, which is the amount that makes a
    /// walk run to an outcome rather than suspend.
    fn unbounded_fuel() -> usize {
        usize::MAX
    }

    /// The observation counts a driver call is expected to leave behind, named so that a failure
    /// says which metric moved.
    ///
    /// `walks_offloaded` is zero throughout, because these tests exercise the inline driver and a
    /// walk it promotes is counted by the task that finishes it.
    fn expected_observations(
        walks_inline: u64,
        error_scan: u64,
        cursor_setup: u64,
        rows: u64,
    ) -> BTreeMap<&'static str, u64> {
        BTreeMap::from([
            ("walks_inline", walks_inline),
            ("walks_offloaded", 0),
            ("error_scan_seconds", error_scan),
            ("cursor_setup_seconds", cursor_setup),
            ("row_iteration_seconds", rows),
            ("row_iteration_rows", rows),
            ("result_sort_seconds", rows),
            ("result_sort_rows", rows),
            ("row_collection_seconds", rows),
        ])
    }

    /// What a driver call answered with, in a form a test can compare whole.
    ///
    /// Mirrors [`PeekStatus`], which carries no comparison of its own because nothing on the peek
    /// path compares one.
    #[derive(Debug, PartialEq)]
    enum Answer {
        NotReady,
        UsePeekStash,
        Promote,
        Ready(PeekResponse),
    }

    impl From<PeekStatus> for Answer {
        fn from(status: PeekStatus) -> Self {
            match status {
                PeekStatus::NotReady => Answer::NotReady,
                PeekStatus::UsePeekStash => Answer::UsePeekStash,
                // The scan a promotion carries has no comparison of its own. What is comparable
                // is that the walk left this driver rather than answering here.
                PeekStatus::Promote(_) => Answer::Promote,
                PeekStatus::Ready(response) => Answer::Ready(response),
            }
        }
    }

    /// An index peek of `peek` over an index holding `keys` and `errors`.
    fn index_peek_over(peek: Peek, keys: &[Row], errors: ErrorUpdates) -> IndexPeek {
        IndexPeek {
            peek,
            trace_bundle: trace_bundle(keys, errors),
            span: tracing::Span::none(),
        }
    }

    /// The rows a completed peek answers with, each at a multiplicity of one and in the order the
    /// walk produced them.
    pub(crate) fn row_collection(rows: impl IntoIterator<Item = Row>) -> RowCollection {
        let rows = rows
            .into_iter()
            .map(|row| (row, NonZeroUsize::new(1).expect("non-zero")))
            .collect();
        RowCollection::new(rows, &[])
    }

    /// The answer a peek gives when its walk completes over `rows`.
    pub(crate) fn rows_answer(rows: impl IntoIterator<Item = Row>) -> PeekResponse {
        PeekResponse::Rows(vec![row_collection(rows)])
    }

    /// A peek whose scan runs to completion is answered with the rows it accumulated, and reports
    /// every phase the walk passed through.
    #[mz_ore::test]
    fn a_completed_scan_answers_with_rows_and_reports_every_phase() {
        let keys: Vec<Row> = (0..6).map(ok_row).collect();
        let mut subject = index_peek_over(
            index_peek(trivial_finishing(), None),
            &keys,
            cancelling_errors(4),
        );
        let metrics = TestMetrics::new();

        let answer = subject.collect_finished_data(
            u64::MAX,
            false,
            usize::MAX,
            None,
            &mut unbounded_fuel(),
            &metrics.as_metrics(),
        );

        assert_eq!(
            Answer::from(answer),
            Answer::Ready(rows_answer((0..6).map(ok_row)))
        );
        assert_eq!(metrics.observations(), expected_observations(1, 1, 1, 1));
    }

    /// A peek its error trace answers reports no phase timer at all, because it reached none of
    /// the phases they measure: the error walk stopped short of the trace's end, and the ok walk
    /// never ran.
    #[mz_ore::test]
    fn an_error_answered_peek_reports_no_phase_timers() {
        let keys: Vec<Row> = (0..6).map(ok_row).collect();
        let (errors, expected) = answering_errors(3);

        let mut subject = index_peek_over(index_peek(trivial_finishing(), None), &keys, errors);
        let metrics = TestMetrics::new();

        let answer = subject.collect_finished_data(
            u64::MAX,
            false,
            usize::MAX,
            None,
            &mut unbounded_fuel(),
            &metrics.as_metrics(),
        );

        assert_eq!(
            Answer::from(answer),
            Answer::Ready(PeekResponse::Error(expected))
        );
        assert_eq!(metrics.observations(), expected_observations(1, 0, 0, 0));
    }

    /// A peek whose accumulated rows fill a batch is diverted to the stash rather than answered
    /// inline. The phases the walk did pass through are reported, and those only a peek answered
    /// inline reaches are not.
    #[mz_ore::test]
    fn a_scan_that_fills_a_batch_diverts_the_peek_to_the_stash() {
        let keys: Vec<Row> = (0..6).map(ok_row).collect();
        let mut subject = index_peek_over(
            index_peek(trivial_finishing(), None),
            &keys,
            cancelling_errors(4),
        );
        let metrics = TestMetrics::new();

        // A threshold of zero bytes is crossed by the first row, so the scan fills a batch well
        // before the trace runs out.
        let answer = subject.collect_finished_data(
            u64::MAX,
            true,
            0,
            None,
            &mut unbounded_fuel(),
            &metrics.as_metrics(),
        );

        assert_eq!(Answer::from(answer), Answer::UsePeekStash);
        assert_eq!(metrics.observations(), expected_observations(1, 1, 1, 0));
    }

    /// A peek whose walk both fills a batch and runs out of fuel is diverted to the stash rather
    /// than promoted, and the driver that diverted it accounts for the walk.
    ///
    /// This is the livelock the placement policy is built around. A promoted scan holding a full
    /// batch has nowhere to write it, so stepping it spends no fuel and advances no cursor, and a
    /// driver that resumed it would yield forever. The two causes of a suspension coincide here,
    /// which is the case a promotion condition written as "the fuel ran out" would get wrong.
    #[mz_ore::test]
    fn a_batch_ready_suspension_is_diverted_rather_than_promoted() {
        let keys: Vec<Row> = (0..6).map(ok_row).collect();
        let mut subject = index_peek_over(
            index_peek(trivial_finishing(), None),
            &keys,
            cancelling_errors(0),
        );
        let metrics = TestMetrics::new();

        // A threshold of zero bytes is crossed by the first row the ok walk produces. One
        // position reaches the end of the empty error trace and the second produces that row, so
        // the scan suspends holding a full batch and out of fuel, with both causes of a
        // suspension in force at once.
        let mut fuel = 2;
        let answer = subject.collect_finished_data(
            u64::MAX,
            true,
            0,
            None,
            &mut fuel,
            &metrics.as_metrics(),
        );
        assert_eq!(Answer::from(answer), Answer::UsePeekStash);
        assert_eq!(fuel, 0, "the slice spent every position it was given");
        assert_eq!(metrics.observations(), expected_observations(1, 1, 1, 0));
    }

    /// A peek whose walk outruns the fuel it was granted leaves the worker rather than being
    /// answered or diverted, and the walk it leaves with reports nothing.
    ///
    /// Reporting here as well as in the driver that finishes the walk would count one walk twice,
    /// on both substrates and in every phase histogram, and the numbers a scan carries are
    /// cumulative precisely so that the driver which ends it can report all of them.
    #[mz_ore::test]
    fn a_scan_that_outruns_its_fuel_leaves_the_worker_reporting_nothing() {
        let keys: Vec<Row> = (0..6).map(ok_row).collect();
        let mut subject = index_peek_over(
            index_peek(trivial_finishing(), None),
            &keys,
            cancelling_errors(0),
        );
        let metrics = TestMetrics::new();

        // An empty error trace is walked out within a position or two, so this fuel is spent
        // inside the ok walk with most of the six keys still ahead of it.
        let mut fuel = 2;
        let answer = subject.collect_finished_data(
            u64::MAX,
            false,
            usize::MAX,
            None,
            &mut fuel,
            &metrics.as_metrics(),
        );

        assert_eq!(Answer::from(answer), Answer::Promote);
        assert_eq!(
            fuel, 0,
            "a promoted slice spent every position it was given"
        );
        assert_eq!(metrics.observations(), expected_observations(0, 0, 0, 0));
    }

    /// A peek its ok walk fails is answered with that error, and reports the phases that walk
    /// reached: the error scan and the cursor setup, both of which precede it, and none of the
    /// histograms an inline answer observes into.
    ///
    /// The sqllogictest sweeps compare answers, so they say nothing about which histogram a
    /// failing peek moved. This is what says that the two timers a clean error walk earns are
    /// observed on the way to the failure rather than after it.
    #[mz_ore::test]
    fn an_ok_phase_failure_reports_the_phases_the_walk_reached() {
        let keys: Vec<Row> = (0..6).map(ok_row).collect();
        let mut subject = index_peek_over(
            index_peek(trivial_finishing(), None),
            &keys,
            cancelling_errors(4),
        );
        let metrics = TestMetrics::new();

        // A ceiling of one byte is crossed by the first row the ok walk produces, so the peek
        // fails inside that walk rather than in the error walk before it.
        let max_result_size = 1;
        let answer = subject.collect_finished_data(
            max_result_size,
            false,
            usize::MAX,
            None,
            &mut unbounded_fuel(),
            &metrics.as_metrics(),
        );

        assert_eq!(
            Answer::from(answer),
            Answer::Ready(PeekResponse::Error(PeekError::unstructured(format!(
                "result exceeds max size of {}",
                ByteSize::b(max_result_size)
            ))))
        );
        assert_eq!(metrics.observations(), expected_observations(1, 1, 1, 0));
    }
}

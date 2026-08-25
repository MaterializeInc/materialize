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
mod peek_metrics;
mod peek_offload;
mod peek_result_iterator;
mod peek_scan;
mod peek_stash;

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
        let traces = TraceManager::new(metrics.clone());
        let command_history = ComputeCommandHistory::new(metrics.for_history());
        let peek_walk_metrics = PeekWalkMetrics::new(&metrics);

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
            worker_config: mz_dyncfgs::all_dyncfgs().into(),
            metrics_registry,
            workers_per_process,
            peek_permits,
            peek_walk_metrics,
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
    fn process_peek(&mut self, upper: &mut Antichain<Timestamp>, mut peek: PendingPeek) {
        let response = match &mut peek {
            PendingPeek::Index(peek) => {
                let start = Instant::now();

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

                let status = peek.seek_fulfillment(
                    upper,
                    self.compute_state.max_result_size,
                    peek_stash_enabled && peek_stash_eligible,
                    peek_stash_threshold_bytes,
                    row_iteration_limit,
                    &metrics,
                );

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
        let mut upper = Antichain::new();
        let pending_peeks = std::mem::take(&mut self.compute_state.pending_peeks);
        for (_uuid, peek) in pending_peeks {
            self.process_peek(&mut upper, peek);
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
    fn seek_fulfillment(
        &mut self,
        upper: &mut Antichain<Timestamp>,
        max_result_size: u64,
        peek_stash_eligible: bool,
        peek_stash_threshold_bytes: usize,
        row_iteration_limit: Option<usize>,
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
            metrics,
        );

        metrics
            .seek_fulfillment_seconds
            .observe(method_start.elapsed().as_secs_f64());

        result
    }

    /// Answers the peek by scanning the traces that fulfil it.
    ///
    /// One call drives one scan to an answer and drops it, so nothing survives the call and a
    /// second call repeats both walks from the start. That is what the peek path asks for: it
    /// reaches this only once the frontiers admit the read, and every outcome retires the peek
    /// from the index-peek path, either with a response or by handing it to the stash.
    fn collect_finished_data(
        &mut self,
        max_result_size: u64,
        peek_stash_eligible: bool,
        peek_stash_threshold_bytes: usize,
        row_iteration_limit: Option<usize>,
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

        // No caller supplies a budget, so nothing stops the scan short of an answer but a full
        // batch, and this driver has nowhere to write one.
        let mut fuel = usize::MAX;
        let outcome = scan.step(row_iteration_limit, &mut fuel);

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

/// Tests of the inline index-peek driver, and the fixtures a peek scan is tested over.
///
/// The fixtures are shared with [`peek_scan`]'s own tests, which build a scan from the same
/// [`TraceBundle`] this driver hands one.
#[cfg(test)]
pub(crate) mod index_peek_tests {
    use differential_dataflow::operators::arrange::TraceAgent;
    use differential_dataflow::trace::{Batcher, Builder, Trace};
    use mz_expr::RowSetFinishing;
    use mz_repr::{Datum, Diff, RelationDesc};
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
        Peek {
            target: PeekTarget::Index { id: TARGET_ID },
            result_desc: RelationDesc::empty(),
            literal_constraints,
            uuid: Uuid::nil(),
            timestamp: PEEK_TIMESTAMP,
            finishing,
            map_filter_project,
            otel_ctx: OpenTelemetryContext::empty(),
        }
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

    /// The rows a completed peek over `values` answers with.
    fn row_collection(values: impl IntoIterator<Item = u8>) -> RowCollection {
        let rows = values
            .into_iter()
            .map(|value| (ok_row(value), NonZeroUsize::new(1).expect("non-zero")))
            .collect();
        RowCollection::new(rows, &[])
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

        let answer =
            subject.collect_finished_data(u64::MAX, false, usize::MAX, None, &metrics.as_metrics());

        assert_eq!(
            Answer::from(answer),
            Answer::Ready(PeekResponse::Rows(vec![row_collection(0..6)]))
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

        let answer =
            subject.collect_finished_data(u64::MAX, false, usize::MAX, None, &metrics.as_metrics());

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
        let answer = subject.collect_finished_data(u64::MAX, true, 0, None, &metrics.as_metrics());

        assert_eq!(Answer::from(answer), Answer::UsePeekStash);
        assert_eq!(metrics.observations(), expected_observations(1, 1, 1, 0));
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

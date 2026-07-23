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
use differential_dataflow::lattice::{Lattice, antichain_join};
use differential_dataflow::trace::cursor::BatchCursor;
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{Cursor, Navigable, TraceReader};
use mz_compute_client::logging::LoggingConfig;
use mz_compute_client::protocol::command::{
    ComputeCommand, ComputeParameters, InstanceConfig, Peek, PeekTarget,
};
use mz_compute_client::protocol::history::ComputeCommandHistory;
use mz_compute_client::protocol::response::{
    ComputeResponse, CopyToResponse, FrontiersResponse, PeekResponse, SubscribeResponse,
};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::dyncfgs::{
    ENABLE_PEEK_RESPONSE_STASH, PEEK_RESPONSE_STASH_BATCH_MAX_RUNS,
    PEEK_RESPONSE_STASH_THRESHOLD_BYTES, PEEK_STASH_BATCH_SIZE, PEEK_STASH_NUM_BATCHES,
};
use mz_compute_types::plan::render_plan::RenderPlan;
use mz_dyncfg::ConfigSet;
use mz_expr::row::RowCollection;
use mz_expr::{RowComparator, SafeMfpPlan};
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
use mz_repr::fixed_length::ExtendDatums;
use mz_repr::{DatumVec, Diff, GlobalId, Row, RowArena, Timestamp};
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

use crate::arrangement::manager::{PaddedTrace, TraceBundle, TraceManager};
use crate::logging;
use crate::logging::compute::{CollectionLogging, ComputeEvent, PeekEvent};
use crate::logging::initialize::LoggingTraces;
use crate::metrics::{CollectionMetrics, WorkerMetrics};
use crate::render::{LinearJoinSpec, StartSignal};
use crate::server::{ComputeInstanceContext, ComputeRuntimeRole, ResponseSender};
use crate::sharing::{ArrangementSharingRegistry, SharedErrsHandle, SharedOksHandle};
use crate::typedefs::ErrAgent;

mod peek_result_iterator;
mod peek_stash;

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
    ///
    /// The maintenance runtime's peeks (local-trace index, persist, stash) live here and are
    /// retired by the every-step `process_peeks` poll. Interactive shared-index peeks do NOT use
    /// this map. They resolve through the notification-driven `pending_work`/`dep_index` store
    /// below.
    pub pending_peeks: BTreeMap<Uuid, PendingPeek>,
    /// Interactive-runtime deferred fast-path peeks, keyed by a fresh `WorkId`, owning each item
    /// until it resolves.
    ///
    /// Empty on the maintenance runtime. An item is re-examined only when its target index id is
    /// marked dirty in the sharing registry (publication or seal) and the worker is woken. See
    /// `resolve_dirty`. Query dataflows do not defer, they build immediately and bind their imports
    /// through registry placeholders, so only peeks live here.
    pub pending_work: BTreeMap<WorkId, PendingWork>,
    /// Dependency id to the set of `pending_work` items waiting on it.
    ///
    /// Empty on the maintenance runtime. On a dirty event for an id, exactly the `WorkId`s indexed
    /// here under that id are re-examined, so wakeups scale with what changed, not with total
    /// pending work.
    pub dep_index: BTreeMap<GlobalId, BTreeSet<WorkId>>,
    /// Monotonic counter minting the next `WorkId` for `pending_work`.
    next_work_id: u64,
    /// The persist location where we can stash large peek results.
    pub peek_stash_persist_location: Option<PersistLocation>,
    /// The logger, from Timely's logging framework, if logs are enabled.
    pub compute_logger: Option<logging::compute::Logger>,
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    /// This is intentionally shared between workers.
    pub persist_clients: Arc<PersistClientCache>,
    /// A per-process registry of published index arrangements.
    ///
    /// Intentionally shared between all workers of the process, each of which publishes into its own
    /// worker-ordinal slot. `Clone` shares the same underlying map.
    pub sharing_registry: ArrangementSharingRegistry,
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

    /// Which of the process's compute runtimes this state belongs to.
    ///
    /// Only the maintenance runtime runs the non-idempotent process-global initializers. The
    /// interactive runtime shares the same process and inherits those globals.
    role: ComputeRuntimeRole,
}

impl ComputeState {
    /// Construct a new `ComputeState`.
    pub fn new(
        role: ComputeRuntimeRole,
        persist_clients: Arc<PersistClientCache>,
        sharing_registry: ArrangementSharingRegistry,
        txns_ctx: TxnsContext,
        metrics: WorkerMetrics,
        tracing_handle: Arc<TracingHandle>,
        context: ComputeInstanceContext,
        metrics_registry: MetricsRegistry,
        workers_per_process: usize,
        storage_log_reader: Option<crate::server::StorageTimelyLogReader>,
    ) -> Self {
        let traces = TraceManager::new(metrics.clone());
        let command_history = ComputeCommandHistory::new(metrics.for_history());

        Self {
            collections: Default::default(),
            traces,
            subscribe_response_buffer: Default::default(),
            copy_to_response_buffer: Default::default(),
            pending_peeks: Default::default(),
            pending_work: Default::default(),
            dep_index: Default::default(),
            next_work_id: 0,
            peek_stash_persist_location: None,
            compute_logger: None,
            persist_clients,
            sharing_registry,
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
            suspended_collections: Default::default(),
            server_maintenance_interval: Duration::ZERO,
            init_system_time: mz_ore::now::SYSTEM_TIME(),
            replica_expiration: Antichain::default(),
            storage_log_reader,
            role,
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

    /// Which of the process's compute runtimes this state serves.
    pub(crate) fn role(&self) -> ComputeRuntimeRole {
        self.role
    }

    /// Mints the next `WorkId` for `pending_work`.
    fn next_work_id(&mut self) -> WorkId {
        let id = self.next_work_id;
        self.next_work_id += 1;
        WorkId(id)
    }

    /// Stores `peek` in `pending_work`, indexing it under its target index id so a publication or
    /// seal event for that id re-examines it.
    fn enqueue_shared_peek(&mut self, peek: SharedIndexPeek) {
        let dep = peek.peek.target.id();
        let work_id = self.next_work_id();
        self.pending_work.insert(work_id, PendingWork::Peek(peek));
        self.dep_index.entry(dep).or_default().insert(work_id);
    }

    /// Removes `work_id` from every dependency's waiter set, dropping now-empty sets. Called once a
    /// pending item is served or cancelled.
    fn clear_dep_index(&mut self, work_id: WorkId) {
        self.dep_index.retain(|_dep, waiters| {
            waiters.remove(&work_id);
            !waiters.is_empty()
        });
    }

    /// Apply the current `worker_config` to the compute state.
    fn apply_worker_config(&mut self) {
        use mz_compute_types::dyncfgs::*;

        let config = &self.worker_config;

        self.linear_join_spec = LinearJoinSpec::from_config(config);

        // lgalloc is process-global. Only the maintenance runtime configures it; the interactive
        // runtime shares the same process and inherits maintenance's configuration.
        if self.role.owns_process_globals() {
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

        // The memory limiter and the columnation lgalloc region flag are process-global. Only
        // maintenance configures them; the interactive runtime inherits maintenance's settings.
        if self.role.owns_process_globals() {
            crate::memory_limiter::apply_limiter_config(config);

            mz_ore::region::ENABLE_LGALLOC_REGION.store(
                ENABLE_COLUMNATION_LGALLOC.get(config),
                std::sync::atomic::Ordering::Relaxed,
            );
        }

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

        // Remember the maintenance interval locally to avoid reading it from the config set on
        // every server iteration.
        self.server_maintenance_interval = COMPUTE_SERVER_MAINTENANCE_INTERVAL.get(config);

        // `set_behavior` mutates a process-global. Only maintenance applies it; the interactive
        // runtime inherits the behavior maintenance installs.
        if self.role.owns_process_globals() {
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
        // replica's arrangements. `DICTIONARY_COMPRESSION` is process-global. Only the maintenance
        // runtime stores it; the interactive runtime shares the process and inherits the value, and
        // both runtimes host a single instance, so this single store covers all arrangements.
        if self.compute_state.role.owns_process_globals() {
            mz_row_spine::DICTIONARY_COMPRESSION.store(
                config.arrangement_dictionary_compression,
                std::sync::atomic::Ordering::Relaxed,
            );
        }

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
        // Every dataflow builds immediately, in command arrival order. Timely allocates per-worker
        // channel ids in construction order, so deferring a build until its dependencies publish
        // would diverge that order across workers, latently unsound under a multi-worker interactive
        // runtime. On the interactive runtime a query dataflow imports its maintenance-index inputs
        // from the sharing registry, binding each through a registry placeholder that a maintenance
        // publisher adopts later (see `render::import_shared_index`). A not-yet-published dependency
        // therefore yields an empty import held at the minimum frontier, so the build is always
        // possible without waiting.
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
        //
        // Every dataflow builds immediately on `CreateDataflow`, inserting its
        // `suspended_collections` entry before the `Schedule` that follows in arrival order can
        // reach us. A `Schedule` with no entry is therefore a stray or duplicate command, a silent
        // no-op.
        if let Some(suspension_token) = self.compute_state.suspended_collections.remove(&id) {
            drop(suspension_token);
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
                if self.compute_state.role == ComputeRuntimeRole::Interactive {
                    // The interactive runtime maintains no traces of its own; the maintenance
                    // runtime publishes them into the per-process sharing registry. This peek is
                    // routed below to `serve_or_defer_shared_peek`, which serves it inline if the
                    // arrangement is published and sealed, else enqueues it for notification-driven
                    // resolution. The maintenance runtime keeps using the local `TraceManager`
                    // below, unchanged.
                    let worker_index = self.timely_worker.index();
                    let registry = self.compute_state.sharing_registry.clone();
                    let max_result_size = self.compute_state.max_result_size;
                    PendingPeek::index_shared(peek, registry, worker_index, max_result_size)
                } else {
                    // Acquire a copy of the trace suitable for fulfilling the peek.
                    let trace_bundle = self.compute_state.traces.get(id).unwrap().clone();
                    PendingPeek::index(peek, trace_bundle)
                }
            }
            PeekTarget::Persist { metadata, .. } => {
                let metadata = metadata.clone();
                PendingPeek::persist(
                    peek,
                    Arc::clone(&self.compute_state.persist_clients),
                    metadata,
                    usize::cast_from(self.compute_state.max_result_size),
                    self.timely_worker,
                )
            }
        };

        // Log the receipt of the peek.
        if let Some(logger) = self.compute_state.compute_logger.as_mut() {
            logger.log(&pending.as_log_event(true));
        }

        // The interactive runtime's shared-index peek resolves through the notification-driven
        // pending-work store, not the every-step `pending_peeks` poll. `IndexShared` is produced
        // only on the interactive runtime (see the `handle_peek` match above), so this branch
        // captures exactly that case.
        if let PendingPeek::IndexShared(shared) = pending {
            self.serve_or_defer_shared_peek(shared);
        } else {
            self.process_peek(&mut Antichain::new(), pending);
        }
    }

    /// Runs the inline registry walk for `peek` once. Returns `None` if it was served (the response
    /// has been sent), or `Some(peek)` if the arrangement is not yet published or sealed and the
    /// caller should keep it pending.
    ///
    /// Shared by the first inline attempt (`serve_or_defer_shared_peek`) and every notification-
    /// driven re-attempt (`resolve_dirty`), so both walk the registry identically.
    fn serve_shared_peek_once(&mut self, peek: SharedIndexPeek) -> Option<SharedIndexPeek> {
        let mut upper = Antichain::new();
        match shared_index_peek_response(
            &peek.registry,
            peek.worker_index,
            &peek.peek,
            peek.max_result_size,
            &mut upper,
        ) {
            PeekStatus::Ready(response) => {
                let _span =
                    span!(parent: &peek.span, Level::DEBUG, "process_peek_response").entered();
                self.send_peek_response(PendingPeek::IndexShared(peek), response);
                None
            }
            PeekStatus::NotReady => Some(peek),
            PeekStatus::UsePeekStash => {
                unreachable!("the interactive peek is never peek-stash eligible")
            }
        }
    }

    /// Serves a shared-index peek inline if its index is published and sealed, otherwise enqueues it
    /// in `pending_work` to be re-examined on a publication or seal event.
    fn serve_or_defer_shared_peek(&mut self, peek: SharedIndexPeek) {
        if let Some(peek) = self.serve_shared_peek_once(peek) {
            self.compute_state.enqueue_shared_peek(peek);
        }
    }

    /// Re-examines only the pending work waiting on a dirtied dependency id.
    ///
    /// Called by the interactive server loop on wake with the drained dirty set. Each affected peek
    /// re-runs its inline registry walk exactly once. A now-ready peek is served and dropped. One
    /// still not ready stays enqueued, so a later event re-examines it. This is the
    /// notification-driven replacement for the every-step `pending_peeks` scan.
    pub(crate) fn resolve_dirty(&mut self, dirty: BTreeSet<GlobalId>) {
        // Collect the affected `WorkId`s first, so re-examination does not read the index while it
        // is being mutated.
        let mut work_ids = BTreeSet::new();
        for dep in &dirty {
            if let Some(waiters) = self.compute_state.dep_index.get(dep) {
                work_ids.extend(waiters.iter().copied());
            }
        }

        for work_id in work_ids {
            // Take the item so its owned data can drive the walk. Re-inserted below if not ready.
            let Some(work) = self.compute_state.pending_work.remove(&work_id) else {
                continue;
            };
            match work {
                PendingWork::Peek(peek) => match self.serve_shared_peek_once(peek) {
                    None => self.compute_state.clear_dep_index(work_id),
                    Some(peek) => {
                        // Still waiting: its `dep_index` entries are untouched, so a later event
                        // re-examines it.
                        self.compute_state
                            .pending_work
                            .insert(work_id, PendingWork::Peek(peek));
                    }
                },
            }
        }
    }

    fn handle_cancel_peek(&mut self, uuid: Uuid) {
        if let Some(peek) = self.compute_state.pending_peeks.remove(&uuid) {
            self.send_peek_response(peek, PeekResponse::Canceled);
            return;
        }
        // Interactive shared-index peeks live in `pending_work`, keyed by `WorkId`. Find the one
        // carrying this uuid, drop it from the store and its dep index, and report the cancellation.
        // A scan over pending work is fine here: a cancel is an event, not a per-step poll.
        let work_id =
            self.compute_state
                .pending_work
                .iter()
                .find_map(|(work_id, work)| match work {
                    PendingWork::Peek(peek) if peek.peek.uuid == uuid => Some(*work_id),
                    PendingWork::Peek(_) => None,
                });
        if let Some(work_id) = work_id {
            let Some(PendingWork::Peek(peek)) = self.compute_state.pending_work.remove(&work_id)
            else {
                return;
            };
            self.compute_state.clear_dep_index(work_id);
            self.send_peek_response(PendingPeek::IndexShared(peek), PeekResponse::Canceled);
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
        // Drop any published arrangement for this index from the sharing registry. Done
        // unconditionally rather than gated on `enable_index_arrangement_sharing`, so a slot
        // published while the flag was on is still reclaimed if the flag is later turned off. The
        // call is a no-op when nothing was published for `id`.
        self.compute_state.sharing_registry.remove(&id);
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

        let mut config = config;
        // The interactive runtime maintains no introspection indexes of its own: it serves
        // introspection peeks from the maintenance runtime's registry-published copies (see
        // `logging::publish_logging_index`). Force logging off so its replay stays empty. The
        // dataflows still install (empty, per database-issues#4545), so the logging indexes are
        // still created and the sanity check below still holds, but they hold no data and, being
        // non-maintenance, are never published.
        if self.compute_state.role() == ComputeRuntimeRole::Interactive {
            config.enable_logging = false;
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
            self.compute_state.role(),
            self.compute_state.sharing_registry.clone(),
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

        // The interactive runtime installs empty copies of the maintenance runtime's
        // logging/introspection indexes (see `initialize_logging`) and shares every non-transient
        // collection's identity with the maintenance runtime, which owns and reports the real
        // frontiers. Reporting our empty copies' frontiers races the owner's report for the same
        // collection id in the controller's single per-collection frontier stream, regressing it.
        // Report only the wholly-transient query dataflows this runtime exclusively hosts.
        let report_only_transient = self.compute_state.role() == ComputeRuntimeRole::Interactive;

        // Maintain a single allocation for `new_frontier` to avoid allocating on every iteration.
        let mut new_frontier = Antichain::new();

        for (&id, collection) in self.compute_state.collections.iter_mut() {
            // The compute protocol does not allow `Frontiers` responses for subscribe and copy-to
            // collections (database-issues#4701).
            if collection.is_subscribe_or_copy {
                continue;
            }

            if report_only_transient && !id.is_transient() {
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
                    error_scan_seconds: &self.compute_state.metrics.index_peek_error_scan_seconds,
                    cursor_setup_seconds: &self
                        .compute_state
                        .metrics
                        .index_peek_cursor_setup_seconds,
                    row_iteration_seconds: &self
                        .compute_state
                        .metrics
                        .index_peek_row_iteration_seconds,
                    result_sort_seconds: &self.compute_state.metrics.index_peek_result_sort_seconds,
                    row_collection_seconds: &self
                        .compute_state
                        .metrics
                        .index_peek_row_collection_seconds,
                };

                let status = peek.seek_fulfillment(
                    upper,
                    self.compute_state.max_result_size,
                    peek_stash_enabled && peek_stash_eligible,
                    peek_stash_threshold_bytes,
                    &metrics,
                );

                self.compute_state
                    .metrics
                    .index_peek_total_seconds
                    .observe(start.elapsed().as_secs_f64());

                match status {
                    PeekStatus::Ready(result) => Some(result),
                    PeekStatus::NotReady => None,
                    PeekStatus::UsePeekStash => {
                        let _span =
                            span!(parent: &peek.span, Level::DEBUG, "process_stash_peek").entered();

                        let peek_stash_batch_max_runs = PEEK_RESPONSE_STASH_BATCH_MAX_RUNS
                            .get(&self.compute_state.worker_config);

                        let stash_task = peek_stash::StashingPeek::start_upload(
                            Arc::clone(&self.compute_state.persist_clients),
                            self.compute_state
                                .peek_stash_persist_location
                                .as_ref()
                                .expect("verified above"),
                            peek.peek.clone(),
                            peek.trace_bundle.clone(),
                            peek_stash_batch_max_runs,
                        );

                        self.compute_state
                            .pending_peeks
                            .insert(peek.peek.uuid, PendingPeek::Stash(stash_task));
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
            PendingPeek::IndexShared(_) => {
                // Interactive shared-index peeks never reach `process_peek`. `handle_peek` routes
                // them to `serve_or_defer_shared_peek`, and they are re-examined only by
                // `resolve_dirty` on a notification, never by the `pending_peeks` poll.
                unreachable!(
                    "interactive shared-index peeks resolve via pending_work, not process_peek"
                )
            }
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

/// Identifies one item in `ComputeState::pending_work`.
///
/// Opaque and minted by `ComputeState::next_work_id`, so the id index can name a waiting item
/// without borrowing it.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct WorkId(u64);

/// An interactive-runtime item deferred until its registry dependency resolves.
///
/// A fast-path index peek waits on two events (publication, seal) keyed by `GlobalId`, resolving
/// through the notification path and id index. A query dataflow does not defer: it builds immediately
/// and binds its imports through registry placeholders (see `handle_create_dataflow`), so only peeks
/// live here.
pub enum PendingWork {
    /// A fast-path index peek, served inline once its target index is published and sealed.
    Peek(SharedIndexPeek),
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
    /// An interactive-runtime peek served inline from the arrangement sharing registry.
    IndexShared(SharedIndexPeek),
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

    /// Builds an interactive-runtime index peek served inline from the sharing registry.
    ///
    /// The maintenance runtime publishes its indexes into the per-process registry; the interactive
    /// runtime serves peeks from there. `handle_peek` attempts it once inline via
    /// [`shared_index_peek_response`]. If the arrangement is not yet published or its upper has not
    /// sealed the peek timestamp, it is enqueued in `pending_work` and re-examined only when a
    /// publication or seal marks its target id dirty. No worker-blocking wait, no spawned task.
    fn index_shared(
        peek: Peek,
        registry: ArrangementSharingRegistry,
        worker_index: usize,
        max_result_size: u64,
    ) -> Self {
        PendingPeek::IndexShared(SharedIndexPeek {
            peek,
            registry,
            worker_index,
            max_result_size,
            span: tracing::Span::current(),
        })
    }

    fn persist(
        peek: Peek,
        persist_clients: Arc<PersistClientCache>,
        metadata: CollectionMetadata,
        max_result_size: usize,
        timely_worker: &TimelyWorker,
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
                )
                .await
            } else {
                Ok(vec![])
            };
            let result = match result {
                Ok(rows) => PeekResponse::Rows(vec![RowCollection::new(rows, &order_by)]),
                Err(e) => PeekResponse::Error(e.to_string()),
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
            PendingPeek::IndexShared(p) => &p.span,
        }
    }

    pub(crate) fn peek(&self) -> &Peek {
        match self {
            PendingPeek::Index(p) => &p.peek,
            PendingPeek::Persist(p) => &p.peek,
            PendingPeek::Stash(p) => &p.peek,
            PendingPeek::IndexShared(p) => &p.peek,
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
    ) -> Result<Vec<(Row, NonZeroUsize)>, String> {
        let client = persist_clients
            .open(metadata.persist_location)
            .await
            .map_err(|e| e.to_string())?;

        let mut reader: ReadHandle<SourceData, (), Timestamp, StorageDiff> = client
            .open_leased_reader(
                metadata.data_shard,
                Arc::new(metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics::from_purpose("persist::peek"),
                USE_CRITICAL_SINCE_SNAPSHOT.get(client.dyncfgs()),
            )
            .await
            .map_err(|e| e.to_string())?;

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
            format!("attempted to peek at {as_of}, but the since has advanced to {since:?}")
        })?;

        // Re-used state for processing and building rows.
        let mut result = vec![];
        let mut datum_vec = DatumVec::new();
        let mut row_builder = Row::default();
        let arena = RowArena::new();
        let mut total_size = 0usize;

        let literal_len = match &literal_constraint {
            None => 0,
            Some(row) => row.iter().count(),
        };

        'collect: while limit_remaining > 0 {
            let Some(batch) = cursor.next().await else {
                break;
            };
            for (data, _, d) in batch {
                let row = data.map_err(|e| e.to_string())?;

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
                    format!(
                        "Invalid data in source, \
                         saw retractions ({}) for row that does not exist: {:?}",
                        -d, row,
                    )
                })?;
                let Some(count) = NonZeroUsize::new(count) else {
                    continue;
                };
                let mut datum_local = datum_vec.borrow_with(&row);
                let eval_result = mfp_plan
                    .evaluate_into(&mut datum_local, &arena, &mut row_builder)
                    .map(|row| row.cloned())
                    .map_err(|e| e.to_string())?;
                if let Some(row) = eval_result {
                    total_size = total_size
                        .saturating_add(row.byte_len())
                        .saturating_add(std::mem::size_of::<NonZeroUsize>());
                    if total_size > max_result_size {
                        return Err(format!(
                            "result exceeds max size of {}",
                            ByteSize::b(u64::cast_from(max_result_size))
                        ));
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

/// An interactive-runtime index peek served inline from the arrangement sharing registry.
///
/// [`shared_index_peek_response`] resolves it against the registry handles. When enqueued in
/// `pending_work`, `resolve_dirty` re-runs that walk on each publication or seal event for the
/// target id until the arrangement is published and its upper has sealed the peek timestamp. It
/// holds only the data that inline walk needs, with no spawned task or blocking wait.
///
/// Note that `PendingPeek` intentionally does not implement or derive `Clone`, as each
/// `PendingPeek` is meant to be dropped after it's responded to.
pub struct SharedIndexPeek {
    pub(crate) peek: Peek,
    /// The per-process registry the maintenance runtime publishes its arrangements into.
    registry: ArrangementSharingRegistry,
    /// The worker ordinal whose published slot this peek reads.
    worker_index: usize,
    /// Max size in bytes of the peek result.
    max_result_size: u64,
    /// The `tracing::Span` tracking this peek's operation.
    span: tracing::Span,
}

/// Histogram metrics for index peek phases.
///
/// This struct bundles references to the various histogram metrics used to
/// instrument the index peek processing pipeline.
pub(crate) struct IndexPeekMetrics<'a> {
    pub seek_fulfillment_seconds: &'a prometheus::Histogram,
    pub frontier_check_seconds: &'a prometheus::Histogram,
    pub error_scan_seconds: &'a prometheus::Histogram,
    pub cursor_setup_seconds: &'a prometheus::Histogram,
    pub row_iteration_seconds: &'a prometheus::Histogram,
    pub result_sort_seconds: &'a prometheus::Histogram,
    pub row_collection_seconds: &'a prometheus::Histogram,
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
            return PeekStatus::Ready(PeekResponse::Error(error));
        }

        metrics
            .frontier_check_seconds
            .observe(method_start.elapsed().as_secs_f64());

        // Once we reach this point the peek is known-fulfillable: the frontier and
        // compaction-frontier checks above have passed.
        let result = self.collect_finished_data(
            max_result_size,
            peek_stash_eligible,
            peek_stash_threshold_bytes,
            metrics,
        );

        metrics
            .seek_fulfillment_seconds
            .observe(method_start.elapsed().as_secs_f64());

        result
    }

    /// Collects data for a known-complete peek from the ok stream.
    fn collect_finished_data(
        &mut self,
        max_result_size: u64,
        peek_stash_eligible: bool,
        peek_stash_threshold_bytes: usize,
        metrics: &IndexPeekMetrics<'_>,
    ) -> PeekStatus {
        let error_scan_start = Instant::now();

        // Check if there exist any errors and, if so, return whatever one we
        // find first.
        let (cursor, storage) = self.trace_bundle.errs_mut().cursor();
        if let Some(error) = Self::scan_errs_for_error::<PaddedTrace<ErrAgent<Timestamp, Diff>>>(
            self.peek.target.id(),
            self.peek.timestamp,
            cursor,
            storage,
        ) {
            return PeekStatus::Ready(error);
        }

        metrics
            .error_scan_seconds
            .observe(error_scan_start.elapsed().as_secs_f64());

        Self::collect_ok_finished_data(
            &self.peek,
            self.trace_bundle.oks_mut(),
            max_result_size,
            peek_stash_eligible,
            peek_stash_threshold_bytes,
            metrics,
        )
    }

    /// Scans an errs cursor for any error at or before `peek_timestamp`, returning the first one
    /// found (or `None`).
    ///
    /// Shared between the maintenance errs scan in `collect_finished_data` (cursor borrowed live off
    /// a local `TraceBundle`) and the interactive inline scan in `shared_index_peek_response`
    /// (cursor off a registry `SharedErrsHandle`): a `(cursor, storage)` pair looks the same to this
    /// scan either way.
    fn scan_errs_for_error<Tr>(
        target_id: GlobalId,
        peek_timestamp: Timestamp,
        mut cursor: peek_result_iterator::TraceCursor<Tr>,
        storage: peek_result_iterator::TraceStorage<Tr>,
    ) -> Option<PeekResponse>
    where
        Tr: TraceReader<Batch: Navigable>,
        for<'a> BatchCursor<Tr>: Cursor<
                Key<'a>: std::fmt::Display,
                TimeGat<'a>: PartialOrder<Timestamp>,
                DiffGat<'a> = &'a Diff,
            >,
    {
        while cursor.key_valid(&storage) {
            let mut copies = Diff::ZERO;
            cursor.map_times(&storage, |time, diff| {
                if time.less_equal(&peek_timestamp) {
                    copies += diff;
                }
            });
            if copies.is_negative() {
                let error = cursor.key(&storage);
                error!(
                    target = %target_id, diff = %copies, %error,
                    "index peek encountered negative multiplicities in error trace",
                );
                return Some(PeekResponse::Error(format!(
                    "Invalid data in source errors, \
                    saw retractions ({}) for row that does not exist: {}",
                    -copies, error,
                )));
            }
            if copies.is_positive() {
                return Some(PeekResponse::Error(cursor.key(&storage).to_string()));
            }
            cursor.step_key(&storage);
        }
        None
    }

    /// Collects data for a known-complete peek from the ok stream.
    fn collect_ok_finished_data<Tr>(
        peek: &Peek,
        oks_handle: &mut Tr,
        max_result_size: u64,
        peek_stash_eligible: bool,
        peek_stash_threshold_bytes: usize,
        metrics: &IndexPeekMetrics<'_>,
    ) -> PeekStatus
    where
        Tr: TraceReader<Batch: Navigable>,
        for<'a> BatchCursor<Tr>: Cursor<
                Key<'a>: ExtendDatums + Eq,
                KeyContainer: BatchContainer<Owned = Row>,
                Val<'a>: ExtendDatums,
                TimeGat<'a>: PartialOrder<Timestamp>,
                DiffGat<'a> = &'a Diff,
            >,
    {
        // Cursor setup timing
        let cursor_setup_start = Instant::now();

        // We clone `literal_constraints` here because we don't want to move the constraints
        // out of the peek struct, and don't want to modify in-place.
        let peek_iterator = peek_result_iterator::PeekResultIterator::new(
            peek.target.id().clone(),
            peek.map_filter_project.clone(),
            peek.timestamp,
            peek.literal_constraints.clone().as_deref_mut(),
            oks_handle,
        );

        metrics
            .cursor_setup_seconds
            .observe(cursor_setup_start.elapsed().as_secs_f64());

        Self::drain_ok_iterator(
            peek_iterator,
            peek,
            max_result_size,
            peek_stash_eligible,
            peek_stash_threshold_bytes,
            Some(metrics),
        )
    }

    /// Drains a [`peek_result_iterator::PeekResultIterator`] into a [`PeekStatus`], sorting and
    /// truncating per `peek.finishing`.
    ///
    /// Shared between the maintenance walk (`collect_ok_finished_data`, iterator borrowed live off a
    /// local `TraceBundle`) and the interactive inline walk (`shared_index_peek_response`, iterator
    /// off a registry `SharedOksHandle`): the accumulation logic is identical either way. Only the
    /// maintenance walk records per-phase metrics; the interactive walk passes `metrics` as `None`.
    fn drain_ok_iterator<Tr>(
        mut peek_iterator: peek_result_iterator::PeekResultIterator<Tr>,
        peek: &Peek,
        max_result_size: u64,
        peek_stash_eligible: bool,
        peek_stash_threshold_bytes: usize,
        metrics: Option<&IndexPeekMetrics<'_>>,
    ) -> PeekStatus
    where
        Tr: TraceReader<Batch: Navigable>,
        for<'a> BatchCursor<Tr>: Cursor<
                Key<'a>: ExtendDatums + Eq,
                KeyContainer: BatchContainer<Owned = Row>,
                Val<'a>: ExtendDatums,
                TimeGat<'a>: PartialOrder<Timestamp>,
                DiffGat<'a> = &'a Diff,
            >,
    {
        let max_result_size = usize::cast_from(max_result_size);
        let count_byte_size = size_of::<NonZeroUsize>();

        // Accumulated `Vec<(row, count)>` results that we are likely to return.
        let mut results = Vec::new();
        let mut total_size: usize = 0;

        // When set, a bound on the number of records we need to return.
        // The requirements on the records are driven by the finishing's
        // `order_by` field. Further limiting will happen when the results
        // are collected, so we don't need to have exactly this many results,
        // just at least those results that would have been returned.
        let max_results = peek.finishing.num_rows_needed();

        let comparator = RowComparator::new(peek.finishing.order_by.as_slice());

        // Row iteration timing
        let row_iteration_start = Instant::now();
        let mut sort_time_accum = Duration::ZERO;

        while let Some(row) = peek_iterator.next() {
            let row: (Row, _) = match row {
                Ok(row) => row,
                Err(err) => return PeekStatus::Ready(PeekResponse::Error(err)),
            };
            let (row, copies) = row;
            let copies: NonZeroUsize = NonZeroUsize::try_from(copies).expect("fits into usize");

            total_size = total_size
                .saturating_add(row.byte_len())
                .saturating_add(count_byte_size);
            if peek_stash_eligible && total_size > peek_stash_threshold_bytes {
                return PeekStatus::UsePeekStash;
            }
            if total_size > max_result_size {
                return PeekStatus::Ready(PeekResponse::Error(format!(
                    "result exceeds max size of {}",
                    ByteSize::b(u64::cast_from(max_result_size))
                )));
            }

            results.push((row, copies));

            // If we hold many more than `max_results` records, we can thin down
            // `results` using `self.finishing.ordering`.
            if let Some(max_results) = max_results {
                // We use a threshold twice what we intend, to amortize the work
                // across all of the insertions. We could tighten this, but it
                // works for the moment.
                if results.len() >= 2 * max_results {
                    if peek.finishing.order_by.is_empty() {
                        results.truncate(max_results);
                        if let Some(metrics) = metrics {
                            metrics
                                .row_iteration_seconds
                                .observe(row_iteration_start.elapsed().as_secs_f64());
                            metrics
                                .result_sort_seconds
                                .observe(sort_time_accum.as_secs_f64());
                        }
                        let row_collection_start = Instant::now();
                        let collection = RowCollection::new(results, &peek.finishing.order_by);
                        if let Some(metrics) = metrics {
                            metrics
                                .row_collection_seconds
                                .observe(row_collection_start.elapsed().as_secs_f64());
                        }
                        return PeekStatus::Ready(PeekResponse::Rows(vec![collection]));
                    } else {
                        // We can sort `results` and then truncate to `max_results`.
                        // This has an effect similar to a priority queue, without
                        // its interactive dequeueing properties.
                        // TODO: Had we left these as `Vec<Datum>` we would avoid
                        // the unpacking; we should consider doing that, although
                        // it will require a re-pivot of the code to branch on this
                        // inner test (as we prefer not to maintain `Vec<Datum>`
                        // in the other case).
                        let sort_start = Instant::now();
                        results.sort_by(|left, right| {
                            comparator.compare_rows(&left.0, &right.0, || left.0.cmp(&right.0))
                        });
                        sort_time_accum += sort_start.elapsed();
                        let dropped = results.drain(max_results..);
                        let dropped_size =
                            dropped
                                .into_iter()
                                .fold(0, |acc: usize, (row, _count): (Row, _)| {
                                    acc.saturating_add(
                                        row.byte_len().saturating_add(count_byte_size),
                                    )
                                });
                        total_size = total_size.saturating_sub(dropped_size);
                    }
                }
            }
        }

        if let Some(metrics) = metrics {
            metrics
                .row_iteration_seconds
                .observe(row_iteration_start.elapsed().as_secs_f64());
            metrics
                .result_sort_seconds
                .observe(sort_time_accum.as_secs_f64());
        }

        let row_collection_start = Instant::now();
        let collection = RowCollection::new(results, &peek.finishing.order_by);
        if let Some(metrics) = metrics {
            metrics
                .row_collection_seconds
                .observe(row_collection_start.elapsed().as_secs_f64());
        }
        PeekStatus::Ready(PeekResponse::Rows(vec![collection]))
    }
}

/// Serves an interactive-runtime fast-path index peek inline from the arrangement sharing registry,
/// mirroring the local [`IndexPeek::seek_fulfillment`] gate over the registry handles.
///
/// Returns [`PeekStatus::NotReady`] (so the caller enqueues the peek in `pending_work` for a later
/// notification-driven re-examination) whenever the arrangement is not yet published, or its upper
/// has not sealed the peek timestamp. Both conditions are transient and clear once the maintenance
/// runtime publishes and advances the index, so deferral covers the reconciliation race (a `Peek`
/// replayed before the maintenance runtime re-publishes its index) without blocking the worker.
///
/// A timestamp compacted past the arrangement's logical-compaction frontier yields the same
/// compaction-frontier error the local path returns.
fn shared_index_peek_response(
    registry: &ArrangementSharingRegistry,
    worker_index: usize,
    peek: &Peek,
    max_result_size: u64,
    upper: &mut Antichain<Timestamp>,
) -> PeekStatus {
    let id = peek.target.id();

    // Non-blocking: an unpublished arrangement defers rather than blocking the worker. Retried on
    // the next `process_peeks`.
    let Some((mut oks, mut errs)) = registry.handles(&id, worker_index) else {
        return PeekStatus::NotReady;
    };

    // Same seal gate as the local `seek_fulfillment`: while an element of the upper is not beyond
    // the peek timestamp the result can still change, so defer.
    oks.read_upper(upper);
    if upper.less_equal(&peek.timestamp) {
        return PeekStatus::NotReady;
    }
    errs.read_upper(upper);
    if upper.less_equal(&peek.timestamp) {
        return PeekStatus::NotReady;
    }

    // Same compaction-frontier gate and error string as the local path, over the meet of the two
    // handles' logical-compaction frontiers (the local path takes the same meet in
    // `TraceBundle::compaction_frontier`).
    let read_frontier = antichain_join(
        &oks.get_logical_compaction(),
        &errs.get_logical_compaction(),
    );
    if !read_frontier.less_equal(&peek.timestamp) {
        let error = format!(
            "Arrangement compaction frontier ({:?}) is beyond the time of the attempted read ({})",
            read_frontier.elements(),
            peek.timestamp,
        );
        return PeekStatus::Ready(PeekResponse::Error(error));
    }

    let (errs_cursor, errs_storage) = errs.cursor();
    if let Some(error) = IndexPeek::scan_errs_for_error::<SharedErrsHandle>(
        id,
        peek.timestamp,
        errs_cursor,
        errs_storage,
    ) {
        return PeekStatus::Ready(error);
    }

    let (oks_cursor, oks_storage) = oks.cursor();
    let peek_iterator =
        peek_result_iterator::PeekResultIterator::<SharedOksHandle>::new_over_cursor(
            id,
            peek.map_filter_project.clone(),
            peek.timestamp,
            peek.literal_constraints.clone().as_deref_mut(),
            oks_cursor,
            oks_storage,
        );

    // The interactive peek is never peek-stash eligible (stash_eligible = false), so draining
    // always resolves to `Ready`.
    IndexPeek::drain_ok_iterator(peek_iterator, peek, max_result_size, false, 0, None)
}

#[cfg(test)]
mod index_peek_tests {
    use std::rc::Rc;

    use differential_dataflow::input::Input;
    use differential_dataflow::operators::arrange::TraceAgent;
    use differential_dataflow::trace::{Builder, Description, Trace};
    use mz_compute_types::dataflows::{BuildDesc, IndexDesc};
    use mz_compute_types::plan::LirRelationExpr;
    use mz_expr::{
        AggregateExpr, AggregateFunc, MapFilterProject, MirRelationExpr, MirScalarExpr,
        OptimizedMirRelationExpr, RowSetFinishing,
    };
    use mz_repr::optimize::OptimizerFeatures;
    use mz_repr::{Datum, RelationDesc, ReprRelationType, SqlScalarType};
    use mz_row_spine::{RowRowBatcher, RowRowBuilder};
    use mz_timely_util::columnation::{ColumnationChunker, ColumnationStack};
    use timely::container::PushInto;
    use timely::dataflow::operators::generic::OperatorInfo;
    use timely::progress::Timestamp as _;
    use uuid::Uuid;

    use mz_persist_client::cache::PersistClientCache;
    use mz_secrets::{InMemorySecretsController, SecretsController};
    use mz_storage_types::connections::ConnectionContext;
    use mz_txn_wal::operator::TxnsContext;

    use super::*;
    use crate::extensions::arrange::{KeyCollection, MzArrange};
    use crate::render::errors::DataflowErrorSer;
    use crate::shared_trace::PublishArrangement;
    use crate::sharing::SharedIndexArrangement;
    use crate::typedefs::{ErrAgent, ErrBatcher, ErrBuilder, ErrSpine, RowRowAgent, RowRowSpine};

    fn row(x: i64) -> Row {
        Row::pack_slice(&[Datum::Int64(x)])
    }

    /// Builds a one-batch `[0, upper)` oks trace with `rows`, wrapped exactly like a real
    /// index's `TraceBundle.oks` (a `PaddedTrace<RowRowAgent<..>>`), but constructed directly
    /// (bypassing rendering a dataflow) for test purposes.
    ///
    /// The batch is inserted through the `TraceWriter` (not `Trace::insert` on the bare spine
    /// directly), because the writer tracks its own idea of the trace's current upper and
    /// asserts new batches are contiguous with it; inserting straight into the spine before
    /// wrapping desyncs that bookkeeping, and the writer's `Drop` (which seals the trace to the
    /// empty frontier) then panics. Closing the trace this way is fine for a test snapshot: an
    /// empty (fully closed) upper is readable at any finite peek timestamp.
    fn oks_trace_with_rows(
        upper: Timestamp,
        rows: Vec<((Row, Row), Timestamp, Diff)>,
    ) -> PaddedTrace<RowRowAgent<Timestamp, Diff>> {
        let spine: RowRowSpine<Timestamp, Diff> =
            Trace::new(OperatorInfo::new(0, 0, Rc::from(vec![0])), None, None);
        let (agent, mut writer) =
            TraceAgent::new(spine, OperatorInfo::new(1, 0, Rc::from(vec![0])), None);

        let description = Description::new(
            Antichain::from_elem(Timestamp::minimum()),
            Antichain::from_elem(upper),
            Antichain::from_elem(Timestamp::minimum()),
        );
        let mut chunk = ColumnationStack::default();
        for row in rows {
            chunk.push_into(row);
        }
        let batch = RowRowBuilder::<Timestamp, Diff>::seal(&mut vec![chunk], description);
        writer.insert(batch, Some(Timestamp::minimum()));

        agent.into()
    }

    /// Builds a one-batch `[0, upper)` errs trace with no errors, wrapped like a real index's
    /// `TraceBundle.errs`.
    fn errs_trace_empty(upper: Timestamp) -> PaddedTrace<ErrAgent<Timestamp, Diff>> {
        let spine: ErrSpine<Timestamp, Diff> =
            Trace::new(OperatorInfo::new(2, 0, Rc::from(vec![0])), None, None);
        let (agent, mut writer) =
            TraceAgent::new(spine, OperatorInfo::new(3, 0, Rc::from(vec![0])), None);

        let description = Description::new(
            Antichain::from_elem(Timestamp::minimum()),
            Antichain::from_elem(upper),
            Antichain::from_elem(Timestamp::minimum()),
        );
        let chunk = ColumnationStack::default();
        let batch = ErrBuilder::<Timestamp, Diff>::seal(&mut vec![chunk], description);
        writer.insert(batch, Some(Timestamp::minimum()));

        agent.into()
    }

    fn test_metrics(registry: &mz_ore::metrics::MetricsRegistry) -> crate::metrics::ComputeMetrics {
        crate::metrics::ComputeMetrics::register_with(registry, ComputeRuntimeRole::Maintenance)
    }

    fn make_peek(timestamp: Timestamp) -> Peek {
        let result_desc = RelationDesc::builder()
            .with_column("k", SqlScalarType::Int64.nullable(false))
            .with_column("v", SqlScalarType::Int64.nullable(false))
            .finish();
        Peek {
            target: PeekTarget::Index {
                id: GlobalId::User(1),
            },
            result_desc,
            literal_constraints: None,
            uuid: Uuid::new_v4(),
            timestamp,
            finishing: RowSetFinishing::trivial(2),
            map_filter_project: MapFilterProject::new(2)
                .into_plan()
                .expect("identity MFP plans")
                .into_nontemporal()
                .expect("identity MFP has no temporal filters"),
            otel_ctx: OpenTelemetryContext::empty(),
        }
    }

    fn index_metrics(metrics: &crate::metrics::WorkerMetrics) -> IndexPeekMetrics<'_> {
        IndexPeekMetrics {
            seek_fulfillment_seconds: &metrics.index_peek_seek_fulfillment_seconds,
            frontier_check_seconds: &metrics.index_peek_frontier_check_seconds,
            error_scan_seconds: &metrics.index_peek_error_scan_seconds,
            cursor_setup_seconds: &metrics.index_peek_cursor_setup_seconds,
            row_iteration_seconds: &metrics.index_peek_row_iteration_seconds,
            result_sort_seconds: &metrics.index_peek_result_sort_seconds,
            row_collection_seconds: &metrics.index_peek_row_collection_seconds,
        }
    }

    /// Publishes `rows` (at time 0, sealed to 1) as a real index arrangement into a fresh registry
    /// under `id` on worker 0 of 1, mirroring how a maintained index publishes on the maintenance
    /// runtime.
    fn publish_kv_index(id: GlobalId, rows: Vec<(Row, Row)>) -> ArrangementSharingRegistry {
        let registry = ArrangementSharingRegistry::new();
        publish_kv_index_into(&registry, id, rows);
        registry
    }

    /// Like [`publish_kv_index`], but publishes into an existing `registry`.
    fn publish_kv_index_into(
        registry: &ArrangementSharingRegistry,
        id: GlobalId,
        rows: Vec<(Row, Row)>,
    ) {
        let registry_in = registry.clone();
        timely::execute_directly(move |worker| {
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (mut oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
                let oks = oks_collection.mz_arrange::<
                    ColumnationChunker<_>,
                    RowRowBatcher<_, _>,
                    RowRowBuilder<_, _>,
                    RowRowSpine<_, _>,
                >("test oks");
                let published_oks = PublishArrangement::publish(&oks);

                let (mut errs_input, errs_collection) =
                    scope.new_collection::<DataflowErrorSer, Diff>();
                let errs = KeyCollection::from(errs_collection).mz_arrange::<
                    ColumnationChunker<_>,
                    ErrBatcher<_, _>,
                    ErrBuilder<_, _>,
                    ErrSpine<_, _>,
                >("test errs");
                let published_errs = PublishArrangement::publish(&errs);

                registry_in.insert(
                    id,
                    0,
                    1,
                    SharedIndexArrangement {
                        oks: published_oks,
                        errs: published_errs,
                    },
                );

                for (k, v) in rows {
                    oks_input.update((k, v), Diff::ONE);
                }
                oks_input.advance_to(Timestamp::from(1_u64));
                oks_input.flush();
                errs_input.advance_to(Timestamp::from(1_u64));
                errs_input.flush();
            });
        });
    }

    /// The interactive inline walk over the sharing registry returns the same `PeekResponse` as the
    /// maintenance runtime's local trace walk over the same rows.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // differential-dataflow's Columnation isn't miri-clean
    fn interactive_shared_peek_matches_local_path() {
        let registry = mz_ore::metrics::MetricsRegistry::new();
        let metrics = test_metrics(&registry).for_worker(0);
        let index_metrics = index_metrics(&metrics);

        let kv = vec![(row(1), row(10)), (row(2), row(20)), (row(3), row(30))];
        let peek_ts = Timestamp::new(0);
        let trace_upper = Timestamp::new(1);

        // The maintenance runtime's local path over an equivalent, locally built trace bundle.
        let mut local_peek = IndexPeek {
            peek: make_peek(peek_ts),
            trace_bundle: TraceBundle::new(
                oks_trace_with_rows(
                    trace_upper,
                    kv.iter()
                        .cloned()
                        .map(|(k, v)| ((k, v), peek_ts, Diff::ONE))
                        .collect(),
                ),
                errs_trace_empty(trace_upper),
            ),
            span: tracing::Span::none(),
        };
        let mut upper = Antichain::new();
        let local_response =
            match local_peek.seek_fulfillment(&mut upper, u64::MAX, false, 0, &index_metrics) {
                PeekStatus::Ready(response) => response,
                _ => panic!("local synchronous walk must resolve directly"),
            };

        // The interactive path: publish the same rows and serve the peek inline off the registry.
        let shared_registry = publish_kv_index(GlobalId::User(1), kv.clone());
        let mut shared_upper = Antichain::new();
        let shared_response = match shared_index_peek_response(
            &shared_registry,
            0,
            &make_peek(peek_ts),
            u64::MAX,
            &mut shared_upper,
        ) {
            PeekStatus::Ready(response) => response,
            _ => panic!("interactive inline walk must resolve ready"),
        };

        assert_eq!(
            local_response, shared_response,
            "shared-registry peek must return the local path's rows"
        );
    }

    /// A local index peek whose timestamp has been compacted past returns a compaction-frontier
    /// error. The interactive inline path mirrors this exact gate over the registry handles, so
    /// this asserts the error string the shared path reproduces.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn seek_fulfillment_compacted_past_errors() {
        let registry = mz_ore::metrics::MetricsRegistry::new();
        let metrics = test_metrics(&registry).for_worker(0);
        let index_metrics = index_metrics(&metrics);

        // A peek at time 1, against a trace that has compacted its logical frontier to time 5: the
        // read is beyond the trace's compaction frontier.
        let peek_timestamp = Timestamp::new(1);
        let trace_upper = Timestamp::new(10);

        let mut bundle = TraceBundle::new(
            oks_trace_with_rows(trace_upper, vec![]),
            errs_trace_empty(trace_upper),
        );
        let compacted = Antichain::from_elem(Timestamp::new(5));
        bundle.oks_mut().set_logical_compaction(compacted.borrow());
        bundle.errs_mut().set_logical_compaction(compacted.borrow());

        let mut peek = IndexPeek {
            peek: make_peek(peek_timestamp),
            trace_bundle: bundle,
            span: tracing::Span::none(),
        };
        let mut upper = Antichain::new();
        let response = match peek.seek_fulfillment(&mut upper, u64::MAX, false, 0, &index_metrics) {
            PeekStatus::Ready(response) => response,
            _ => panic!("a compacted-past read must resolve directly"),
        };
        assert!(
            matches!(&response, PeekResponse::Error(msg) if msg.contains("compaction frontier")),
            "expected a compaction-frontier error, got {response:?}",
        );
    }

    /// A peek for an index that is not yet published defers via `NotReady` (rather than blocking or
    /// erroring), and resolves with the correct rows once the maintenance runtime publishes and the
    /// pending-peek retry runs again.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn interactive_shared_peek_defers_until_published() {
        let id = GlobalId::User(1);
        let kv = vec![(row(1), row(10)), (row(2), row(20))];
        let registry = ArrangementSharingRegistry::new();

        let mut upper = Antichain::new();
        assert!(
            matches!(
                shared_index_peek_response(
                    &registry,
                    0,
                    &make_peek(Timestamp::new(0)),
                    u64::MAX,
                    &mut upper,
                ),
                PeekStatus::NotReady,
            ),
            "an unpublished index must defer",
        );

        // Publishing lets the retry (a later `process_peeks`) resolve the peek.
        publish_kv_index_into(&registry, id, kv.clone());
        let mut upper = Antichain::new();
        assert!(
            matches!(
                shared_index_peek_response(
                    &registry,
                    0,
                    &make_peek(Timestamp::new(0)),
                    u64::MAX,
                    &mut upper,
                ),
                PeekStatus::Ready(PeekResponse::Rows(_)),
            ),
            "a published index must resolve",
        );
    }

    /// A peek at a timestamp the arrangement's upper has not yet sealed defers via `NotReady`, then
    /// resolves once the upper advances past the peek timestamp. Uses a live worker so the
    /// published trace carries a finite (non-empty) upper, which `execute_directly`'s
    /// run-to-completion sealing cannot stage.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn interactive_shared_peek_defers_until_sealed() {
        let id = GlobalId::User(1);
        timely::execute_directly(move |worker| {
            let registry = ArrangementSharingRegistry::new();
            let registry_in = registry.clone();
            let worker_index = worker.index();
            let peers = worker.peers();

            let (mut oks_input, mut errs_input) =
                worker.dataflow::<Timestamp, _, _>(move |scope| {
                    let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
                    let oks = oks_collection.mz_arrange::<
                        ColumnationChunker<_>,
                        RowRowBatcher<_, _>,
                        RowRowBuilder<_, _>,
                        RowRowSpine<_, _>,
                    >("test oks");
                    let published_oks = PublishArrangement::publish(&oks);

                    let (errs_input, errs_collection) =
                        scope.new_collection::<DataflowErrorSer, Diff>();
                    let errs = KeyCollection::from(errs_collection).mz_arrange::<
                        ColumnationChunker<_>,
                        ErrBatcher<_, _>,
                        ErrBuilder<_, _>,
                        ErrSpine<_, _>,
                    >("test errs");
                    let published_errs = PublishArrangement::publish(&errs);

                    registry_in.insert(
                        id,
                        worker_index,
                        peers,
                        SharedIndexArrangement {
                            oks: published_oks,
                            errs: published_errs,
                        },
                    );
                    (oks_input, errs_input)
                });

            // A row at time 0, batch sealed so the trace's upper is {1}.
            oks_input.update((row(1), row(10)), Diff::ONE);
            oks_input.advance_to(Timestamp::from(1_u64));
            oks_input.flush();
            errs_input.advance_to(Timestamp::from(1_u64));
            errs_input.flush();
            for _ in 0..16 {
                worker.step();
            }

            // upper {1} does not seal a peek at time 1: defer.
            let mut upper = Antichain::new();
            assert!(
                matches!(
                    shared_index_peek_response(
                        &registry,
                        worker_index,
                        &make_peek(Timestamp::new(1)),
                        u64::MAX,
                        &mut upper,
                    ),
                    PeekStatus::NotReady,
                ),
                "an unsealed peek must defer",
            );

            // Advance the upper past the peek timestamp; the retry now resolves.
            oks_input.advance_to(Timestamp::from(2_u64));
            oks_input.flush();
            errs_input.advance_to(Timestamp::from(2_u64));
            errs_input.flush();
            for _ in 0..16 {
                worker.step();
            }

            let mut upper = Antichain::new();
            assert!(
                matches!(
                    shared_index_peek_response(
                        &registry,
                        worker_index,
                        &make_peek(Timestamp::new(1)),
                        u64::MAX,
                        &mut upper,
                    ),
                    PeekStatus::Ready(PeekResponse::Rows(_)),
                ),
                "a sealed peek must resolve",
            );

            // Keep the publisher inputs alive until here so the publication stayed open.
            let _keep = (&oks_input, &errs_input);
        });
    }

    fn test_compute_instance_context() -> ComputeInstanceContext {
        ComputeInstanceContext {
            scratch_directory: None,
            worker_core_affinity: false,
            connection_context: ConnectionContext::for_tests(
                InMemorySecretsController::new().reader(),
            ),
        }
    }

    /// Builds a persist client cache inside a Tokio runtime context, which its pubsub task needs.
    /// Returns the runtime too so the caller keeps it alive for the cache's lifetime. The cache is
    /// an `Arc` (so `Send`) and can move into a timely worker closure, unlike the `Rc`-holding
    /// `ComputeState`, which must be built on the worker thread.
    fn test_persist_clients() -> (tokio::runtime::Runtime, Arc<PersistClientCache>) {
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let clients = {
            let _guard = runtime.enter();
            Arc::new(PersistClientCache::new_no_metrics())
        };
        (runtime, clients)
    }

    /// Builds an interactive-runtime `ComputeState` over `registry`, with a fresh, isolated metrics
    /// registry. Enough to drive `handle_peek`/`resolve_dirty` in a test `ActiveComputeState`.
    fn interactive_compute_state(
        persist_clients: Arc<PersistClientCache>,
        registry: ArrangementSharingRegistry,
    ) -> ComputeState {
        let metrics_registry = MetricsRegistry::new();
        let metrics = crate::metrics::ComputeMetrics::register_with(
            &metrics_registry,
            ComputeRuntimeRole::Interactive,
        )
        .for_worker(0);
        ComputeState::new(
            ComputeRuntimeRole::Interactive,
            persist_clients,
            registry,
            TxnsContext::default(),
            metrics,
            Arc::new(TracingHandle::disabled()),
            test_compute_instance_context(),
            metrics_registry,
            1,
            None,
        )
    }

    /// Publishes `rows` as a `RowRow` index under `id` on the CURRENT worker (no nested
    /// `execute_directly`), sealing the batch and draining to the empty upper so the registry's
    /// `Arc` keeps the snapshot readable after the inputs drop.
    fn publish_index_current_worker(
        worker: &mut TimelyWorker,
        registry: &ArrangementSharingRegistry,
        id: GlobalId,
        rows: Vec<(Row, Row)>,
    ) {
        let registry_in = registry.clone();
        let (mut oks_input, mut errs_input) = worker.dataflow::<Timestamp, _, _>(move |scope| {
            let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
            let oks = oks_collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("test oks");
            let published_oks = PublishArrangement::publish(&oks);

            let (errs_input, errs_collection) = scope.new_collection::<DataflowErrorSer, Diff>();
            let errs = KeyCollection::from(errs_collection).mz_arrange::<
                ColumnationChunker<_>,
                ErrBatcher<_, _>,
                ErrBuilder<_, _>,
                ErrSpine<_, _>,
            >("test errs");
            let published_errs = PublishArrangement::publish(&errs);

            registry_in.insert(
                id,
                scope.index(),
                scope.peers(),
                SharedIndexArrangement {
                    oks: published_oks,
                    errs: published_errs,
                },
            );
            (oks_input, errs_input)
        });

        for (k, v) in rows {
            oks_input.update((k, v), Diff::ONE);
        }
        oks_input.advance_to(Timestamp::from(1_u64));
        oks_input.flush();
        errs_input.advance_to(Timestamp::from(1_u64));
        errs_input.flush();
        for _ in 0..16 {
            worker.step();
        }
        // Drop the inputs and drain: the batch seals to the empty upper, readable at any finite ts,
        // and the registry's `Arc` keeps the published chain alive.
        drop(oks_input);
        drop(errs_input);
        for _ in 0..16 {
            worker.step();
        }
    }

    /// A peek issued before its index is published enqueues in `pending_work` (never the maintenance
    /// `pending_peeks` poll path) and is served only when the target id is presented as dirty to
    /// `resolve_dirty`. A re-examination with an empty dirty set, even after the data is published
    /// and ready, serves nothing: this is the no-polling property.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn interactive_peek_resolves_on_publication_not_on_bare_tick() {
        let id = GlobalId::User(1);
        let kv = vec![(row(1), row(10)), (row(2), row(20))];
        // The persist cache spawns a task that needs a Tokio reactor; build it (and keep the
        // runtime alive) before entering the timely worker thread.
        let (_rt, persist_clients) = test_persist_clients();

        timely::execute_directly(move |worker| {
            let registry = ArrangementSharingRegistry::new();
            // Part A: register this interactive worker's waker, as startup does.
            registry.register_waker(0, worker.sync_activator_for([].into()));

            let mut compute_state = interactive_compute_state(persist_clients, registry.clone());
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let mut response_tx = ResponseSender::for_test(tx);

            // A peek issued before publication enqueues, does not respond, and does not touch the
            // poll path.
            {
                let mut active = ActiveComputeState {
                    timely_worker: &mut *worker,
                    compute_state: &mut compute_state,
                    response_tx: &mut response_tx,
                };
                active.handle_peek(make_peek(Timestamp::new(0)));
                assert_eq!(
                    active.compute_state.pending_work.len(),
                    1,
                    "an unpublished peek must enqueue in pending_work"
                );
                assert!(
                    active.compute_state.pending_peeks.is_empty(),
                    "the interactive peek must not use the pending_peeks poll path"
                );
                assert!(
                    active.compute_state.dep_index.contains_key(&id),
                    "the peek must be indexed under its target id"
                );

                // No-polling: with no dirtied id, re-examination serves nothing.
                active.resolve_dirty(BTreeSet::new());
            }
            assert!(rx.try_recv().is_err(), "no response before publication");

            // Publish the index from this same worker. `insert` marks the id dirty for worker 0.
            publish_index_current_worker(worker, &registry, id, kv.clone());

            // No-polling: the data is now published and ready, yet a re-examination with an empty
            // dirty set must NOT serve the peek. Only a dirtied id triggers work.
            {
                let mut active = ActiveComputeState {
                    timely_worker: &mut *worker,
                    compute_state: &mut compute_state,
                    response_tx: &mut response_tx,
                };
                active.resolve_dirty(BTreeSet::new());
            }
            assert!(
                rx.try_recv().is_err(),
                "a bare tick (empty dirty set) must not resolve pending work"
            );

            // The genuine wake: drain the dirty inbox (the id, marked by `insert`) and resolve.
            let dirty = registry.take_dirty(0);
            assert_eq!(
                dirty,
                BTreeSet::from([id]),
                "publication must have marked the id dirty"
            );
            {
                let mut active = ActiveComputeState {
                    timely_worker: &mut *worker,
                    compute_state: &mut compute_state,
                    response_tx: &mut response_tx,
                };
                active.resolve_dirty(dirty);
                assert!(
                    active.compute_state.pending_work.is_empty(),
                    "a served peek is removed from the store"
                );
                assert!(
                    active.compute_state.dep_index.is_empty(),
                    "a served peek clears its dep index"
                );
            }
            let response = match rx.try_recv() {
                Ok((ComputeResponse::PeekResponse(_, response, _), _)) => response,
                other => panic!("expected a peek response, got {other:?}"),
            };
            assert!(
                matches!(response, PeekResponse::Rows(_)),
                "the served peek must carry rows, got {response:?}"
            );
        });
    }

    /// A published-but-not-sealed peek stays enqueued and is served only after a frontier advance
    /// drives `note_frontier` (the seal signal `export_index` wires). A re-examination after the
    /// seal but with no dirty mark serves nothing (no-polling).
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn interactive_peek_resolves_on_seal_via_note_frontier() {
        let id = GlobalId::User(1);
        let (_rt, persist_clients) = test_persist_clients();

        timely::execute_directly(move |worker| {
            let registry = ArrangementSharingRegistry::new();
            let worker_index = worker.index();
            registry.register_waker(worker_index, worker.sync_activator_for([].into()));

            let mut compute_state = interactive_compute_state(persist_clients, registry.clone());
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let mut response_tx = ResponseSender::for_test(tx);

            // Publish a row at time 0, sealing only to upper {1}.
            let registry_in = registry.clone();
            let (mut oks_input, mut errs_input) =
                worker.dataflow::<Timestamp, _, _>(move |scope| {
                    let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
                    let oks = oks_collection.mz_arrange::<
                        ColumnationChunker<_>,
                        RowRowBatcher<_, _>,
                        RowRowBuilder<_, _>,
                        RowRowSpine<_, _>,
                    >("test oks");
                    let published_oks = PublishArrangement::publish(&oks);

                    let (errs_input, errs_collection) =
                        scope.new_collection::<DataflowErrorSer, Diff>();
                    let errs = KeyCollection::from(errs_collection).mz_arrange::<
                        ColumnationChunker<_>,
                        ErrBatcher<_, _>,
                        ErrBuilder<_, _>,
                        ErrSpine<_, _>,
                    >("test errs");
                    let published_errs = PublishArrangement::publish(&errs);

                    registry_in.insert(
                        id,
                        scope.index(),
                        scope.peers(),
                        SharedIndexArrangement {
                            oks: published_oks,
                            errs: published_errs,
                        },
                    );
                    (oks_input, errs_input)
                });

            oks_input.update((row(1), row(10)), Diff::ONE);
            oks_input.advance_to(Timestamp::from(1_u64));
            oks_input.flush();
            errs_input.advance_to(Timestamp::from(1_u64));
            errs_input.flush();
            for _ in 0..16 {
                worker.step();
            }
            // Drain the publication's dirty mark so the seal signal is observed in isolation.
            let _ = registry.take_dirty(worker_index);

            // A peek at ts 1: published but not sealed (upper {1}). Enqueues.
            {
                let mut active = ActiveComputeState {
                    timely_worker: &mut *worker,
                    compute_state: &mut compute_state,
                    response_tx: &mut response_tx,
                };
                active.handle_peek(make_peek(Timestamp::new(1)));
                assert_eq!(
                    active.compute_state.pending_work.len(),
                    1,
                    "an unsealed peek must enqueue"
                );
            }
            assert!(rx.try_recv().is_err(), "an unsealed peek does not respond");

            // Advance the upper past the peek ts and step, so the shared trace seals ts 1.
            oks_input.advance_to(Timestamp::from(2_u64));
            oks_input.flush();
            errs_input.advance_to(Timestamp::from(2_u64));
            errs_input.flush();
            for _ in 0..16 {
                worker.step();
            }

            // No-polling: the seal alone does not re-examine the peek until the id is dirtied.
            {
                let mut active = ActiveComputeState {
                    timely_worker: &mut *worker,
                    compute_state: &mut compute_state,
                    response_tx: &mut response_tx,
                };
                active.resolve_dirty(BTreeSet::new());
            }
            assert!(
                rx.try_recv().is_err(),
                "a seal with no dirty mark must not resolve the peek"
            );

            // The seal signal: `export_index`'s frontier hook calls `note_frontier`. Drive it.
            registry.note_frontier(id, worker_index);
            let dirty = registry.take_dirty(worker_index);
            assert_eq!(dirty, BTreeSet::from([id]));
            {
                let mut active = ActiveComputeState {
                    timely_worker: &mut *worker,
                    compute_state: &mut compute_state,
                    response_tx: &mut response_tx,
                };
                active.resolve_dirty(dirty);
                assert!(
                    active.compute_state.pending_work.is_empty(),
                    "a sealed peek is served and removed"
                );
            }
            let response = match rx.try_recv() {
                Ok((ComputeResponse::PeekResponse(_, response, _), _)) => response,
                other => panic!("expected a peek response, got {other:?}"),
            };
            assert!(
                matches!(response, PeekResponse::Rows(_)),
                "the sealed peek must carry rows, got {response:?}"
            );

            // Keep the publisher inputs alive until here so the publication stayed open.
            let _keep = (&oks_input, &errs_input);
        });
    }

    /// A `(k, v)` `ReprRelationType` of two non-null `int64` columns, matching the rows
    /// [`publish_index_current_worker`] publishes.
    fn two_int64_type() -> ReprRelationType {
        let desc = RelationDesc::builder()
            .with_column("k", SqlScalarType::Int64.nullable(false))
            .with_column("v", SqlScalarType::Int64.nullable(false))
            .finish();
        ReprRelationType::from(desc.typ())
    }

    /// Converts a lowered index-only dataflow into the `<RenderPlan, CollectionMetadata>` shape the
    /// compute protocol ships, mirroring `compute-client`'s `Instance::create_dataflow`. The test
    /// dataflows import only shared indexes (no storage sources) and export no sinks, so the augment
    /// step is trivial.
    fn to_render_dataflow(
        lowered: DataflowDescription<LirRelationExpr, ()>,
    ) -> DataflowDescription<RenderPlan, CollectionMetadata> {
        assert!(
            lowered.source_imports.is_empty(),
            "index-only test dataflow imports no storage sources"
        );
        let objects_to_build = lowered
            .objects_to_build
            .into_iter()
            .map(|o| BuildDesc {
                id: o.id,
                plan: RenderPlan::try_from(o.plan).expect("render plan conversion"),
            })
            .collect();
        DataflowDescription {
            source_imports: BTreeMap::new(),
            objects_to_build,
            index_imports: lowered.index_imports,
            index_exports: lowered.index_exports,
            sink_exports: BTreeMap::new(),
            as_of: lowered.as_of,
            until: lowered.until,
            initial_storage_as_of: lowered.initial_storage_as_of,
            refresh_schedule: lowered.refresh_schedule,
            debug_name: lowered.debug_name,
            time_dependence: lowered.time_dependence,
        }
    }

    /// A real query dataflow that imports the maintenance index `index_id` (arranging `on_id` by
    /// `[0]`) and exports `out_index_id` = `count(*)` over it. Built by lowering hand-written MIR,
    /// exactly as the controller would ship it. No optimization is needed: a reduce lowers
    /// faithfully.
    fn reduce_count_dataflow(
        index_id: GlobalId,
        on_id: GlobalId,
        reduce_id: GlobalId,
        out_index_id: GlobalId,
        as_of: Timestamp,
    ) -> DataflowDescription<RenderPlan, CollectionMetadata> {
        let on_type = two_int64_type();
        let mut mir =
            DataflowDescription::<OptimizedMirRelationExpr, ()>::new("test-reduce".into());
        mir.import_index(
            index_id,
            IndexDesc {
                on_id,
                key: vec![MirScalarExpr::column(0)],
            },
            on_type.clone(),
            false,
        );
        let count = AggregateExpr {
            func: AggregateFunc::Count,
            expr: MirScalarExpr::literal_true(),
            distinct: false,
        };
        let reduce = MirRelationExpr::Reduce {
            input: Box::new(MirRelationExpr::global_get(on_id, on_type)),
            group_key: vec![],
            aggregates: vec![count],
            monotonic: false,
            expected_group_size: None,
        };
        let reduce_type = reduce.typ();
        mir.insert_plan(
            reduce_id,
            OptimizedMirRelationExpr::declare_optimized(reduce),
        );
        mir.set_as_of(Antichain::from_elem(as_of));
        mir.export_index(
            out_index_id,
            IndexDesc {
                on_id: reduce_id,
                key: vec![MirScalarExpr::column(0)],
            },
            reduce_type,
        );
        let lowered = LirRelationExpr::finalize_dataflow(mir, &OptimizerFeatures::default(), None)
            .expect("lowering the reduce dataflow");
        to_render_dataflow(lowered)
    }

    /// A peek over a single-column `int64` result, for reading a `count(*)` query output.
    fn make_count_peek(id: GlobalId, timestamp: Timestamp) -> Peek {
        let result_desc = RelationDesc::builder()
            .with_column("count", SqlScalarType::Int64.nullable(false))
            .finish();
        Peek {
            target: PeekTarget::Index { id },
            result_desc,
            literal_constraints: None,
            uuid: Uuid::new_v4(),
            timestamp,
            finishing: RowSetFinishing::trivial(1),
            map_filter_project: MapFilterProject::new(1)
                .into_plan()
                .expect("identity MFP plans")
                .into_nontemporal()
                .expect("identity MFP has no temporal filters"),
            otel_ctx: OpenTelemetryContext::empty(),
        }
    }

    /// An interactive query dataflow that imports a not-yet-published maintenance index is built
    /// IMMEDIATELY in arrival order. The import binds through a registry placeholder rather than
    /// deferring, so the output collection appears in `collections` right away and nothing lands in
    /// `pending_work`. With the placeholder unadopted, the import produces no data and the output
    /// frontier holds at the minimum, so a result peek at the as_of stays pending.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn interactive_build_is_immediate() {
        let index_id = GlobalId::User(1);
        let on_id = GlobalId::User(2);
        let reduce_id = GlobalId::User(3);
        let out_index_id = GlobalId::User(4);
        let (_rt, persist_clients) = test_persist_clients();

        timely::execute_directly(move |worker| {
            let registry = ArrangementSharingRegistry::new();
            registry.register_waker(0, worker.sync_activator_for([].into()));
            let mut compute_state = interactive_compute_state(persist_clients, registry.clone());
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let mut response_tx = ResponseSender::for_test(tx);

            let dataflow =
                reduce_count_dataflow(index_id, on_id, reduce_id, out_index_id, Timestamp::new(0));

            // The build is NOT deferred, even though the imported index is unpublished: the import
            // binds through a placeholder that a maintenance publisher adopts later.
            {
                let mut active = ActiveComputeState {
                    timely_worker: &mut *worker,
                    compute_state: &mut compute_state,
                    response_tx: &mut response_tx,
                };
                active.handle_create_dataflow(dataflow);
                assert!(
                    active.compute_state.pending_work.is_empty(),
                    "an immediately-built dataflow must not sit in pending_work"
                );
                assert!(
                    active.compute_state.dep_index.is_empty(),
                    "an immediately-built dataflow indexes nothing under its dependency"
                );
                assert!(
                    active.compute_state.collections.contains_key(&out_index_id),
                    "the query output collection is built immediately"
                );
                // Binding the import through get-or-create created a placeholder slot for the
                // unpublished dependency, so its handles now exist.
                assert!(
                    active
                        .compute_state
                        .sharing_registry
                        .handles(&index_id, 0)
                        .is_some(),
                    "the interactive import created a placeholder slot for its dependency"
                );

                // Start the (suspended) dataflow, as a `Schedule` command would.
                active.handle_schedule(out_index_id);
            }

            // Step so the reduce runs over the empty, unadopted placeholder input. Its output frontier
            // is held at the minimum, so it never seals past the peek time.
            for _ in 0..64 {
                worker.step();
            }

            // A result peek at the as_of cannot resolve while the output frontier is held at the
            // minimum: it stays pending rather than returning wrong (empty) rows.
            {
                let mut active = ActiveComputeState {
                    timely_worker: &mut *worker,
                    compute_state: &mut compute_state,
                    response_tx: &mut response_tx,
                };
                active.handle_peek(make_count_peek(out_index_id, Timestamp::new(0)));
                assert_eq!(
                    active.compute_state.pending_work.len(),
                    1,
                    "the result peek stays pending while the output frontier is held at the minimum"
                );
            }
            assert!(
                rx.try_recv().is_err(),
                "no result is produced while the placeholder input is unadopted"
            );

            // Tear down the built dataflow so the worker can shut down. Its import over the never
            // adopted placeholder holds a frontier at the minimum forever, so without dropping it the
            // dataflow never completes and `execute_directly` would wedge on teardown.
            {
                let mut active = ActiveComputeState {
                    timely_worker: &mut *worker,
                    compute_state: &mut compute_state,
                    response_tx: &mut response_tx,
                };
                active.handle_allow_compaction(out_index_id, Antichain::new());
            }
            for _ in 0..16 {
                worker.step();
            }
        });
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

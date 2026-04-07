// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Unified Timely runtime for compute and storage.
//!
//! This module provides a single Timely runtime that runs both compute and storage
//! dataflows, sharing worker threads and communication infrastructure. This enables
//! compute's logging dataflows to automatically capture events from storage dataflows.
//!
//! See `doc/developer/design/20260402_unified_timely_runtime.md` for the design.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Error, anyhow};
use differential_dataflow::trace::ExertionLogic;
use mz_cluster::client::{ClusterClient, TimelyContainer};
use mz_cluster::communication::initialize_networking;
use mz_cluster_client::client::TimelyConfig;
use mz_compute::command_channel;
use mz_compute::metrics::ComputeMetrics;
use mz_compute::server::{
    CommandReceiver, ComputeInstanceContext, NonceChange, ResponseSender, Worker as ComputeWorker,
    spawn_channel_adapter,
};
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_compute_client::service::ComputeClient;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_rocksdb::config::SharedWriteBufferManager;
use mz_storage::internal_control;
use mz_storage::statistics::AggregatedStatistics;
use mz_storage::storage_state::{StorageInstanceContext, StorageState};
use mz_storage_client::client::{StorageClient, StorageCommand, StorageResponse};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::ConnectionContext;
use mz_txn_wal::operator::TxnsContext;
use timely::WorkerConfig;
use timely::communication::Allocate;
use timely::communication::allocator::GenericBuilder;
use timely::communication::allocator::zero_copy::bytes_slab::BytesRefill;
use timely::execute::execute_from;
use timely::worker::Worker as TimelyWorker;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, watch};
use tracing::{info, info_span};
use uuid::Uuid;

use mz_storage::internal_control::DataflowParameters;
use mz_storage::storage_state::async_storage_worker::AsyncStorageWorker;

/// Client builders returned by [`serve`], one for each domain.
pub struct UnifiedClientBuilders {
    /// Builder for compute clients.
    pub compute: Box<dyn Fn() -> Box<dyn ComputeClient> + Send + Sync>,
    /// Builder for storage clients.
    pub storage: Box<dyn Fn() -> Box<dyn StorageClient> + Send + Sync>,
}

/// Lightweight [`ClusterSpec`] impl for compute in the unified runtime.
///
/// This is only used to create [`TimelyContainer`] and [`ClusterClient`] instances.
/// The actual worker logic is in [`run_unified_worker`].
#[derive(Clone)]
struct ComputeSpec;

impl mz_cluster::client::ClusterSpec for ComputeSpec {
    type Command = ComputeCommand;
    type Response = ComputeResponse;
    const NAME: &str = "compute";
    fn run_worker<A: Allocate + 'static>(
        &self,
        _: &mut TimelyWorker<A>,
        _: mpsc::UnboundedReceiver<(Uuid, mpsc::UnboundedReceiver<ComputeCommand>, mpsc::UnboundedSender<ComputeResponse>)>,
    ) {
        unreachable!("unified runtime does not use ClusterSpec::run_worker");
    }
}

/// Lightweight [`ClusterSpec`] impl for storage in the unified runtime.
#[derive(Clone)]
struct StorageSpec;

impl mz_cluster::client::ClusterSpec for StorageSpec {
    type Command = StorageCommand;
    type Response = StorageResponse;
    const NAME: &str = "storage";
    fn run_worker<A: Allocate + 'static>(
        &self,
        _: &mut TimelyWorker<A>,
        _: mpsc::UnboundedReceiver<(Uuid, mpsc::UnboundedReceiver<StorageCommand>, mpsc::UnboundedSender<StorageResponse>)>,
    ) {
        unreachable!("unified runtime does not use ClusterSpec::run_worker");
    }
}

/// Configuration for the unified runtime.
#[derive(Clone)]
struct UnifiedConfig {
    persist_clients: Arc<PersistClientCache>,
    txns_ctx: TxnsContext,
    tracing_handle: Arc<TracingHandle>,
    compute_metrics: ComputeMetrics,
    compute_context: ComputeInstanceContext,
    metrics_registry: MetricsRegistry,
    workers_per_process: usize,
    // Storage config
    storage_metrics: mz_storage::metrics::StorageMetrics,
    now: NowFn,
    connection_context: ConnectionContext,
    storage_instance_context: StorageInstanceContext,
    shared_rocksdb_write_buffer_manager: SharedWriteBufferManager,
}

/// Start the unified Timely runtime for both compute and storage.
pub async fn serve(
    timely_config: TimelyConfig,
    metrics_registry: &MetricsRegistry,
    persist_clients: Arc<PersistClientCache>,
    txns_ctx: TxnsContext,
    tracing_handle: Arc<TracingHandle>,
    now: NowFn,
    connection_context: ConnectionContext,
    compute_context: ComputeInstanceContext,
    storage_context: StorageInstanceContext,
) -> Result<UnifiedClientBuilders, Error> {
    let config = UnifiedConfig {
        persist_clients,
        txns_ctx,
        tracing_handle,
        compute_metrics: ComputeMetrics::register_with(metrics_registry),
        compute_context,
        metrics_registry: metrics_registry.clone(),
        workers_per_process: timely_config.workers,
        storage_metrics: mz_storage::metrics::StorageMetrics::register_with(metrics_registry),
        now,
        connection_context,
        storage_instance_context: storage_context,
        shared_rocksdb_write_buffer_manager: Default::default(),
    };

    let tokio_executor = Handle::current();
    let (compute_container, storage_container) =
        build_unified_cluster(config, timely_config, tokio_executor).await?;

    // Build compute client factory using ClusterClient (intercepts Hello for reconnection).
    let compute_container = Arc::new(Mutex::new(compute_container));
    let compute_builder: Box<dyn Fn() -> Box<dyn ComputeClient> + Send + Sync> =
        Box::new(move || {
            let client: Box<dyn ComputeClient> =
                Box::new(ClusterClient::new(Arc::clone(&compute_container)));
            client
        });

    // Build storage client factory using ClusterClient.
    let storage_container = Arc::new(Mutex::new(storage_container));
    let storage_builder: Box<dyn Fn() -> Box<dyn StorageClient> + Send + Sync> =
        Box::new(move || {
            let client: Box<dyn StorageClient> =
                Box::new(ClusterClient::new(Arc::clone(&storage_container)));
            client
        });

    Ok(UnifiedClientBuilders {
        compute: compute_builder,
        storage: storage_builder,
    })
}

async fn build_unified_cluster(
    config: UnifiedConfig,
    timely_config: TimelyConfig,
    tokio_executor: Handle,
) -> Result<(TimelyContainer<ComputeSpec>, TimelyContainer<StorageSpec>), Error> {
    info!("Building unified timely container with config {timely_config:?}");

    // Create client channels for both domains
    let (compute_client_txs, compute_client_rxs): (Vec<_>, Vec<_>) = (0..timely_config.workers)
        .map(|_| mpsc::unbounded_channel())
        .unzip();
    let compute_client_rxs: Mutex<Vec<_>> =
        Mutex::new(compute_client_rxs.into_iter().map(Some).collect());

    let (storage_client_txs, storage_client_rxs): (Vec<_>, Vec<_>) = (0..timely_config.workers)
        .map(|_| mpsc::unbounded_channel())
        .unzip();
    let storage_client_rxs: Mutex<Vec<_>> =
        Mutex::new(storage_client_rxs.into_iter().map(Some).collect());

    // Initialize networking (same as ClusterSpec::build_cluster).
    // TODO: Add lgalloc support for zero-copy allocations.
    let refill = BytesRefill {
        logic: Arc::new(|size| Box::new(vec![0; size])),
        limit: timely_config.zero_copy_limit,
    };

    let (builders, other) = if timely_config.enable_zero_copy {
        use timely::communication::allocator::zero_copy::allocator_process::ProcessBuilder;
        initialize_networking::<ProcessBuilder>(
            timely_config.workers,
            timely_config.process,
            timely_config.addresses.clone(),
            refill,
            GenericBuilder::ZeroCopyBinary,
        )
        .await?
    } else {
        initialize_networking::<timely::communication::allocator::Process>(
            timely_config.workers,
            timely_config.process,
            timely_config.addresses.clone(),
            refill,
            GenericBuilder::ZeroCopy,
        )
        .await?
    };

    // Configure the worker
    let mut worker_config = WorkerConfig::default();
    if timely_config.arrangement_exert_proportionality > 0 {
        let merge_effort = Some(1000);
        let prop = timely_config.arrangement_exert_proportionality;
        let arc: ExertionLogic = Arc::new(move |layers| {
            let mut prop = prop;
            let layers = layers
                .iter()
                .copied()
                .skip_while(|(_idx, count, _len)| *count == 0);
            let mut first = true;
            for (_idx, count, len) in layers {
                if count > 1 {
                    return merge_effort;
                }
                if !first && prop > 0 && len > 0 {
                    return merge_effort;
                }
                first = false;
                prop /= 2;
            }
            None
        });
        worker_config.set::<ExertionLogic>("differential/default_exert_logic".to_string(), arc);
    }

    let workers = timely_config.workers;
    let worker_guards = execute_from(builders, other, worker_config, move |timely_worker| {
        let worker_idx = timely_worker.index();

        let span = info_span!("timely", name = "unified", worker_id = worker_idx);
        let _span_guard = span.enter();
        let _tokio_guard = tokio_executor.enter();

        let compute_client_rx = compute_client_rxs.lock().unwrap()[worker_idx % workers]
            .take()
            .unwrap();
        let storage_client_rx = storage_client_rxs.lock().unwrap()[worker_idx % workers]
            .take()
            .unwrap();

        run_unified_worker(timely_worker, &config, compute_client_rx, storage_client_rx);
    })
    .map_err(|e| anyhow!(e))?;

    let worker_threads: Vec<_> = worker_guards
        .guards()
        .iter()
        .map(|h| h.thread().clone())
        .collect();

    let compute_container = TimelyContainer {
        client_txs: compute_client_txs,
        worker_guards: Some(worker_guards),
        worker_threads: worker_threads.clone(),
    };
    let storage_container = TimelyContainer {
        client_txs: storage_client_txs,
        worker_guards: None,
        worker_threads,
    };

    Ok((compute_container, storage_container))
}

/// Entry point for each unified worker thread.
fn run_unified_worker<A: Allocate + 'static>(
    timely_worker: &mut TimelyWorker<A>,
    config: &UnifiedConfig,
    compute_client_rx: mpsc::UnboundedReceiver<(
        Uuid,
        mpsc::UnboundedReceiver<ComputeCommand>,
        mpsc::UnboundedSender<ComputeResponse>,
    )>,
    storage_client_rx: mpsc::UnboundedReceiver<(
        Uuid,
        mpsc::UnboundedReceiver<StorageCommand>,
        mpsc::UnboundedSender<StorageResponse>,
    )>,
) {
    let worker_id = timely_worker.index();

    if config.compute_context.worker_core_affinity {
        // Core affinity setting is handled by compute's set_core_affinity.
        // For now, we skip it in the unified path as it's platform-specific.
    }

    // --- Set up compute command channel ---
    let (cmd_tx, cmd_rx) = command_channel::render(timely_worker);
    let (resp_tx, resp_rx) = mpsc::unbounded_channel();
    spawn_channel_adapter(compute_client_rx, cmd_tx, resp_rx, worker_id);

    let compute_metrics = config.compute_metrics.for_worker(worker_id);

    // --- Set up storage channel adapter ---
    // The adapter handles reconnections: it buffers commands until
    // InitializationComplete, then delivers the batch for reconciliation.
    // Ongoing commands and responses flow through persistent channels.
    let (storage_reconcile_tx, storage_reconcile_rx) = std::sync::mpsc::channel();
    let (storage_cmd_tx, storage_cmd_rx) = std::sync::mpsc::channel();
    let (storage_resp_tx, storage_resp_rx) = mpsc::unbounded_channel();
    spawn_storage_channel_adapter(
        storage_client_rx,
        storage_reconcile_tx,
        storage_cmd_tx,
        storage_resp_rx,
        thread::current(),
    );

    // --- Set up storage state ---
    let (internal_cmd_tx, internal_cmd_rx) =
        internal_control::setup_command_sequencer(timely_worker);

    let storage_configuration =
        StorageConfiguration::new(config.connection_context.clone(), mz_dyncfgs::all_dyncfgs());

    let (read_only_tx, read_only_rx) = watch::channel(true);

    let async_worker =
        AsyncStorageWorker::new(thread::current(), Arc::clone(&config.persist_clients));

    let cluster_memory_limit = config.storage_instance_context.cluster_memory_limit;

    let mut storage_state = StorageState {
        source_uppers: BTreeMap::new(),
        source_tokens: BTreeMap::new(),
        metrics: config.storage_metrics.clone(),
        reported_frontiers: BTreeMap::new(),
        ingestions: BTreeMap::new(),
        exports: BTreeMap::new(),
        oneshot_ingestions: BTreeMap::new(),
        now: config.now.clone(),
        timely_worker_index: timely_worker.index(),
        timely_worker_peers: timely_worker.peers(),
        instance_context: config.storage_instance_context.clone(),
        persist_clients: Arc::clone(&config.persist_clients),
        txns_ctx: config.txns_ctx.clone(),
        sink_tokens: BTreeMap::new(),
        sink_write_frontiers: BTreeMap::new(),
        dropped_ids: Vec::new(),
        aggregated_statistics: AggregatedStatistics::new(
            timely_worker.index(),
            timely_worker.peers(),
        ),
        shared_status_updates: Default::default(),
        latest_status_updates: Default::default(),
        initial_status_reported: Default::default(),
        internal_cmd_tx,
        internal_cmd_rx,
        read_only_tx,
        read_only_rx,
        async_worker,
        storage_configuration,
        dataflow_parameters: DataflowParameters::new(
            config.shared_rocksdb_write_buffer_manager.clone(),
            cluster_memory_limit,
        ),
        tracing_handle: Arc::clone(&config.tracing_handle),
        server_maintenance_interval: Duration::ZERO,
    };

    // --- Build a compute Worker (which owns the timely_worker reference) ---
    // We create the compute Worker struct and use it to drive both domains.
    // The storage step methods are called on StorageState directly,
    // borrowing timely_worker from the compute Worker when needed.

    let mut compute_worker = ComputeWorker {
        timely_worker,
        command_rx: CommandReceiver::new(cmd_rx, worker_id),
        response_tx: ResponseSender::new(resp_tx, worker_id),
        compute_state: None,
        metrics: compute_metrics,
        persist_clients: Arc::clone(&config.persist_clients),
        txns_ctx: config.txns_ctx.clone(),
        tracing_handle: Arc::clone(&config.tracing_handle),
        context: config.compute_context.clone(),
        metrics_registry: config.metrics_registry.clone(),
        workers_per_process: config.workers_per_process,
    };

    // --- Run the unified loop ---

    // Wait for first compute nonce (blocks, stepping timely while waiting).
    let NonceChange(nonce) = compute_worker
        .recv_command()
        .expect_err("change to first nonce");
    compute_worker.set_nonce(nonce);

    // Main unified loop.
    // Compute reconnection: handled by spawn_channel_adapter + nonce mechanism.
    // Storage reconnection: handled by spawn_storage_channel_adapter which
    // delivers reconciliation batches through storage_reconcile_rx.
    loop {
        let Err(NonceChange(nonce)) = run_unified_loop(
            &mut compute_worker,
            &mut storage_state,
            &storage_reconcile_rx,
            &storage_cmd_rx,
            &storage_resp_tx,
        );
        compute_worker.set_nonce(nonce);
    }
}

fn run_unified_loop<A: Allocate + 'static>(
    compute: &mut ComputeWorker<'_, A>,
    storage_state: &mut StorageState,
    storage_reconcile_rx: &std::sync::mpsc::Receiver<Vec<StorageCommand>>,
    storage_cmd_rx: &std::sync::mpsc::Receiver<StorageCommand>,
    storage_resp_tx: &mpsc::UnboundedSender<StorageResponse>,
) -> Result<std::convert::Infallible, NonceChange> {
    // Reconcile compute.
    compute.reconcile()?;

    let mut last_compute_maintenance = Instant::now();
    let mut last_storage_maintenance = Instant::now();
    let mut last_stats_time = Instant::now();

    loop {
        // --- Check for storage reconnection ---
        if let Ok(commands) = storage_reconcile_rx.try_recv() {
            // Drain any stale commands from the previous connection before
            // reconciling with the new one.
            while storage_cmd_rx.try_recv().is_ok() {}
            storage_state.reconcile_with_commands(commands);
        }

        // --- Compute maintenance ---
        let compute_maintenance_interval = compute
            .compute_state
            .as_ref()
            .map_or(Duration::ZERO, |state| state.server_maintenance_interval);

        let now = Instant::now();
        let compute_sleep;
        if now >= last_compute_maintenance + compute_maintenance_interval {
            last_compute_maintenance = now;
            compute_sleep = None;

            if let Some(mut cs) = compute.activate_compute() {
                cs.compute_state.traces.maintenance();
                cs.report_frontiers();
                cs.report_metrics();
                cs.check_expiration();
            }
            compute.metrics.record_shared_row_metrics();
        } else {
            let next = last_compute_maintenance + compute_maintenance_interval;
            compute_sleep = Some(next.saturating_duration_since(now));
        }

        // --- Storage maintenance ---
        let storage_maintenance_interval = storage_state.server_maintenance_interval;
        let storage_sleep;
        if now >= last_storage_maintenance + storage_maintenance_interval {
            last_storage_maintenance = now;
            storage_sleep = None;

            storage_state.report_frontier_progress(storage_resp_tx);
        } else {
            let next = last_storage_maintenance + storage_maintenance_interval;
            storage_sleep = Some(next.saturating_duration_since(now));
        }

        // --- Determine park duration ---
        let stats_interval = storage_state
            .storage_configuration
            .parameters
            .statistics_collection_interval;
        let stats_remaining = stats_interval.saturating_sub(last_stats_time.elapsed());

        // The storage channel adapter unparks the worker thread when it sends
        // commands, so we don't need to check storage_cmd_rx here.
        let can_park = compute.command_rx.is_empty()
            && storage_state.async_worker.is_empty();

        let sleep = min_durations(&[compute_sleep, storage_sleep, Some(stats_remaining)]);

        // --- Step Timely ---
        let timer = compute.metrics.timely_step_duration_seconds.start_timer();
        if can_park {
            compute.timely_worker.step_or_park(sleep);
        } else {
            compute.timely_worker.step();
        }
        timer.observe_duration();

        // --- Compute step ---
        compute.handle_pending_commands()?;
        if let Some(mut cs) = compute.activate_compute() {
            cs.process_peeks();
            cs.process_subscribes();
            cs.process_copy_tos();
        }

        // --- Storage step ---
        // Dropped IDs
        for id in std::mem::take(&mut storage_state.dropped_ids) {
            let _ = storage_resp_tx.send(StorageResponse::DroppedId(id));
        }

        // Oneshot ingestions
        storage_state.process_oneshot_ingestions(storage_resp_tx);

        // Status updates
        storage_state.report_status_updates(storage_resp_tx);

        // Statistics
        if last_stats_time.elapsed() >= stats_interval {
            storage_state.report_storage_statistics(storage_resp_tx);
            last_stats_time = Instant::now();
        }

        // Drain storage commands
        while let Ok(cmd) = storage_cmd_rx.try_recv() {
            storage_state.handle_storage_command(cmd);
        }

        // Drain async worker responses
        while let Ok(response) = storage_state.async_worker.try_recv() {
            storage_state.handle_async_worker_response_inner(response);
        }

        // Drain internal commands (these may render dataflows)
        while let Some(cmd) = storage_state.internal_cmd_rx.try_recv() {
            storage_state.handle_internal_storage_command_with(compute.timely_worker, cmd);
        }
    }
}

/// Spawn a task that bridges [`ClusterClient`] connections to persistent worker channels.
///
/// For each new storage client connection:
/// 1. Drains commands until `InitializationComplete`, collecting them into a batch
/// 2. Sends the batch through `reconcile_tx` for the worker to reconcile in one go
/// 3. Forwards ongoing commands through `command_tx`
/// 4. Forwards worker responses from `response_rx` back to the active client
///
/// This ensures the worker thread never blocks waiting for a new client connection.
fn spawn_storage_channel_adapter(
    mut client_rx: mpsc::UnboundedReceiver<(
        Uuid,
        mpsc::UnboundedReceiver<StorageCommand>,
        mpsc::UnboundedSender<StorageResponse>,
    )>,
    reconcile_tx: std::sync::mpsc::Sender<Vec<StorageCommand>>,
    command_tx: std::sync::mpsc::Sender<StorageCommand>,
    mut response_rx: mpsc::UnboundedReceiver<StorageResponse>,
    worker_thread: thread::Thread,
) {
    mz_ore::task::spawn(
        || "storage-channel-adapter",
        async move {
            while let Some((_nonce, mut cmd_rx, resp_tx)) = client_rx.recv().await {
                // Phase 1: Buffer commands until InitializationComplete.
                let mut commands = vec![];
                let init_complete = loop {
                    match cmd_rx.recv().await {
                        Some(StorageCommand::InitializationComplete) => break true,
                        Some(cmd) => commands.push(cmd),
                        None => break false,
                    }
                };
                if !init_complete {
                    continue;
                }

                // Deliver reconciliation batch to the worker.
                if reconcile_tx.send(commands).is_err() {
                    break;
                }
                worker_thread.unpark();

                // Phase 2: Forward ongoing commands and responses.
                loop {
                    tokio::select! {
                        cmd = cmd_rx.recv() => match cmd {
                            Some(cmd) => {
                                if command_tx.send(cmd).is_err() {
                                    return;
                                }
                                worker_thread.unpark();
                            }
                            None => break, // Client disconnected, wait for next.
                        },
                        resp = response_rx.recv() => match resp {
                            Some(resp) => { let _ = resp_tx.send(resp); }
                            None => return, // Worker gone.
                        },
                    }
                }
            }
        },
    );
}

/// Returns the minimum of a set of optional durations.
fn min_durations(durations: &[Option<Duration>]) -> Option<Duration> {
    durations.iter().copied().flatten().min()
}

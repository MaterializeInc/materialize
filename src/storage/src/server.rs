// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interactive dataflow server.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use mz_cluster::client::{ClusterClient, ClusterSpec};
use mz_cluster_client::client::TimelyConfig;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_rocksdb::config::SharedWriteBufferManager;
use mz_storage_client::client::{StorageClient, StorageCommand, StorageResponse};
use mz_storage_types::connections::ConnectionContext;
use mz_timely_util::capture::EventLink;
use mz_txn_wal::operator::TxnsContext;
use timely::logging::TimelyEvent;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::metrics::StorageMetrics;
use crate::storage_state::{StorageInstanceContext, Worker};

/// Configures a dataflow server.
#[derive(Clone)]
struct Config {
    /// `persist` client cache.
    pub persist_clients: Arc<PersistClientCache>,
    /// Context necessary for rendering txn-wal operators.
    pub txns_ctx: TxnsContext,
    /// A process-global handle to tracing configuration.
    pub tracing_handle: Arc<TracingHandle>,
    /// Function to get wall time now.
    pub now: NowFn,
    /// Configuration for source and sink connection.
    pub connection_context: ConnectionContext,
    /// Other configuration for storage instances.
    pub instance_context: StorageInstanceContext,

    /// Metrics for storage
    pub metrics: StorageMetrics,
    /// Shared rocksdb write buffer manager
    pub shared_rocksdb_write_buffer_manager: SharedWriteBufferManager,
    /// Number of timely workers in this process, for local-index computation.
    pub workers_per_process: usize,
    /// Per-worker writers for forwarding timely logging events to compute,
    /// indexed by local worker index.
    pub timely_log_writers: Arc<Mutex<Vec<Option<TimelyLogWriter>>>>,
}

/// Per-worker writer handle for forwarding timely logging events to compute.
pub(crate) type TimelyLogWriter = Arc<EventLink<mz_repr::Timestamp, Vec<(Duration, TimelyEvent)>>>;

/// Initiates a timely dataflow computation, processing storage commands.
pub async fn serve(
    timely_config: TimelyConfig,
    metrics_registry: &MetricsRegistry,
    persist_clients: Arc<PersistClientCache>,
    txns_ctx: TxnsContext,
    tracing_handle: Arc<TracingHandle>,
    now: NowFn,
    connection_context: ConnectionContext,
    instance_context: StorageInstanceContext,
    timely_log_writers: Vec<TimelyLogWriter>,
) -> Result<impl Fn() -> Box<dyn StorageClient> + use<>, anyhow::Error> {
    let workers_per_process = timely_config.workers;
    // Normalize the log-writer vec to exactly one slot per worker in this process.
    // Empty input means logging is disabled; pad with `None` so index-based access is
    // always in bounds.
    let timely_log_writers = if timely_log_writers.is_empty() {
        (0..workers_per_process).map(|_| None).collect()
    } else {
        assert_eq!(timely_log_writers.len(), workers_per_process);
        timely_log_writers.into_iter().map(Some).collect()
    };
    let config = Config {
        persist_clients,
        txns_ctx,
        tracing_handle,
        now,
        connection_context,
        instance_context,
        metrics: StorageMetrics::register_with(metrics_registry),
        // The shared RocksDB `WriteBufferManager` is shared between the workers.
        // It protects (behind a shared mutex) a `Weak` that will be upgraded and shared when the
        // first worker attempts to initialize it.
        shared_rocksdb_write_buffer_manager: Default::default(),
        workers_per_process,
        timely_log_writers: Arc::new(Mutex::new(timely_log_writers)),
    };
    let tokio_executor = tokio::runtime::Handle::current();

    let timely_container = config.build_cluster(timely_config, tokio_executor).await?;
    let timely_container = Arc::new(Mutex::new(timely_container));

    let client_builder = move || {
        let client = ClusterClient::new(Arc::clone(&timely_container));
        let client: Box<dyn StorageClient> = Box::new(client);
        client
    };

    Ok(client_builder)
}

impl ClusterSpec for Config {
    type Command = StorageCommand;
    type Response = StorageResponse;

    const NAME: &str = "storage";

    fn run_worker(
        &self,
        timely_worker: &mut TimelyWorker,
        client_rx: mpsc::UnboundedReceiver<(
            Uuid,
            mpsc::UnboundedReceiver<StorageCommand>,
            mpsc::UnboundedSender<StorageResponse>,
        )>,
    ) {
        // Register a timely logger that forwards events to the compute logging dataflow.
        // Assign by local worker index so storage worker x matches compute worker x.
        let local_index = timely_worker.index() % self.workers_per_process;
        let writer = self.timely_log_writers.lock().unwrap()[local_index].take();
        if let Some(writer) = writer {
            use timely::dataflow::operators::capture::{Event, EventPusher};
            use timely::logging::TimelyEventBuilder;

            // We use an approach similar to compute's logging: wrap the writer in
            // a BatchLogger that translates Logger callbacks into Event pushes,
            // then register the Logger with timely's log_register.
            let interval_ms = 1000u128; // 1 second batching interval
            let mut time_ms = mz_repr::Timestamp::from(0u64);
            let mut event_pusher = writer;
            let now = std::time::Instant::now();
            let start_offset = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .expect("Failed to get duration since Unix epoch");

            let logger = timely::logging_core::Logger::<TimelyEventBuilder>::new(
                now,
                start_offset,
                move |time: &std::time::Duration,
                      data: &mut Option<Vec<(std::time::Duration, TimelyEvent)>>| {
                    if let Some(data) = data.take() {
                        event_pusher.push(Event::Messages(time_ms, data));
                    } else {
                        // Advance progress.
                        let new_time_ms: u64 = (((time.as_millis() / interval_ms) + 1)
                            * interval_ms)
                            .try_into()
                            .expect("must fit");
                        let new_time_ms = mz_repr::Timestamp::from(new_time_ms);
                        if time_ms < new_time_ms {
                            event_pusher
                                .push(Event::Progress(vec![(new_time_ms, 1), (time_ms, -1)]));
                            time_ms = new_time_ms;
                        }
                    }
                },
            );

            let mut register = timely_worker
                .log_register()
                .expect("Logging must be enabled");
            register.insert_logger("timely", logger);
        }

        Worker::new(
            timely_worker,
            client_rx,
            self.metrics.clone(),
            self.now.clone(),
            self.connection_context.clone(),
            self.instance_context.clone(),
            Arc::clone(&self.persist_clients),
            self.txns_ctx.clone(),
            Arc::clone(&self.tracing_handle),
            self.shared_rocksdb_write_buffer_manager.clone(),
        )
        .run();
    }
}

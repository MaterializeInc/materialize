// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interactive dataflow server.

use std::sync::Arc;
use std::thread::Thread;

use timely::communication::initialize::WorkerGuards;

use mz_cluster::server::TimelyContainerRef;
use mz_ore::now::NowFn;
use mz_persist_client::cache::PersistClientCache;
use mz_storage_client::client::StorageClient;
use mz_storage_client::client::{StorageCommand, StorageResponse};
use mz_storage_client::types::connections::ConnectionContext;
use timely::worker::Worker as TimelyWorker;

use crate::sink::SinkBaseMetrics;
use crate::source::metrics::SourceBaseMetrics;
use crate::storage_state::Worker;
use crate::DecodeMetrics;

/// Configures a dataflow server.
#[derive(Clone)]
pub struct Config {
    /// Function to get wall time now.
    pub now: NowFn,
    /// Configuration for source and sink connection.
    pub connection_context: ConnectionContext,

    /// Metrics for sources.
    pub source_metrics: SourceBaseMetrics,
    /// Metrics for sinks.
    pub sink_metrics: SinkBaseMetrics,
    /// Metrics for decoding.
    pub decode_metrics: DecodeMetrics,
}

/// A handle to a running dataflow server.
///
/// Dropping this object will block until the dataflow computation ceases.
pub struct Server {
    _worker_guards: WorkerGuards<()>,
}

/// Initiates a timely dataflow computation, processing storage commands.
pub fn serve(
    generic_config: mz_cluster::server::ClusterConfig,
    now: NowFn,
    connection_context: ConnectionContext,
) -> Result<
    (
        TimelyContainerRef<StorageCommand, StorageResponse, Thread>,
        impl Fn() -> Box<dyn StorageClient>,
    ),
    anyhow::Error,
> {
    // Various metrics related things.
    let source_metrics = SourceBaseMetrics::register_with(&generic_config.metrics_registry);
    let sink_metrics = SinkBaseMetrics::register_with(&generic_config.metrics_registry);
    let decode_metrics = DecodeMetrics::register_with(&generic_config.metrics_registry);

    let config = Config {
        now,
        connection_context,
        source_metrics,
        sink_metrics,
        decode_metrics,
    };

    let (timely_container, client_builder) = mz_cluster::server::serve::<
        Config,
        StorageCommand,
        StorageResponse,
    >(generic_config, config)?;
    let client_builder = {
        move || {
            let client: Box<dyn StorageClient> = client_builder();
            client
        }
    };

    Ok((timely_container, client_builder))
}

impl mz_cluster::types::AsRunnableWorker<StorageCommand, StorageResponse> for Config {
    type Activatable = std::thread::Thread;
    fn build_and_run<A: timely::communication::Allocate>(
        config: Self,
        timely_worker: &mut TimelyWorker<A>,
        client_rx: crossbeam_channel::Receiver<(
            crossbeam_channel::Receiver<StorageCommand>,
            tokio::sync::mpsc::UnboundedSender<StorageResponse>,
            crossbeam_channel::Sender<std::thread::Thread>,
        )>,
        persist_clients: Arc<PersistClientCache>,
    ) {
        Worker::new(
            timely_worker,
            client_rx,
            config.decode_metrics,
            config.source_metrics,
            config.sink_metrics,
            config.now.clone(),
            config.connection_context,
            persist_clients,
        )
        .run();
    }
}

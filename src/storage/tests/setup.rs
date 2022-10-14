// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Unit tests for sources.

use std::collections::{BTreeMap, HashMap};
use std::marker::{Send, Sync};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use timely::progress::{Antichain, Timestamp as _};

use mz_build_info::DUMMY_BUILD_INFO;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::task::RuntimeExt;
use mz_repr::TimestampManipulation;
use mz_repr::{Diff, GlobalId, Timestamp};
use mz_storage::protocol::client::StorageCommand;
use mz_storage::sink::SinkBaseMetrics;
use mz_storage::source::metrics::SourceBaseMetrics;
use mz_storage::source::testscript::ScriptCommand;
use mz_storage::types::sources::{
    encoding::SourceDataEncoding, SourceConnection, SourceData, SourceDesc, SourceEnvelope,
    TestScriptSourceConnection,
};
use mz_storage::DecodeMetrics;

pub fn assert_source_results_in(
    source: Vec<ScriptCommand>,
    encoding: SourceDataEncoding,
    envelope: SourceEnvelope,
    expected_values: Vec<SourceData>,
) -> Result<(), anyhow::Error> {
    let timestamp_interval = Duration::from_secs(1);

    let desc = SourceDesc {
        connection: SourceConnection::TestScript(TestScriptSourceConnection {
            desc_json: serde_json::to_string(&source).unwrap(),
        }),
        encoding,
        envelope,
        metadata_columns: vec![],
        timestamp_interval,
    };

    build_and_run_source(desc, timestamp_interval, move |upper, mut rh| {
        let expected_values = expected_values.clone();
        async move {
            let as_of = if let Some(cur_time) = upper.as_option().copied() {
                Antichain::from_elem(cur_time.step_back().unwrap_or_else(Timestamp::minimum))
            } else {
                // terminated source, we try to fetch everything
                Antichain::from_elem(Timestamp::maximum())
            };
            let mut snapshot = rh.snapshot_and_fetch(as_of).await.unwrap();

            // retry until we have enough data.
            //
            // TODO(guswynn): have a builtin timeout here for a source
            // not producing enough data.
            if snapshot.len() < expected_values.len() {
                false
            } else {
                differential_dataflow::consolidation::consolidate_updates(&mut snapshot);

                let values: Vec<SourceData> = snapshot
                    .into_iter()
                    .map(|((key, value), _time, diff)| {
                        assert_eq!(diff, 1);
                        assert_eq!(value, Ok(()));
                        // unwrap any errors from persist
                        key.unwrap()
                    })
                    .collect();

                assert_eq!(values, expected_values);

                true
            }
        }
    })
}

/// Setups up a single-worker dataflow for the given `SourceDesc` and
/// runs it until the `until` future returns `True`
fn build_and_run_source<F, Fut>(
    desc: SourceDesc,
    timestamp_interval: Duration,
    until: F,
) -> Result<(), anyhow::Error>
where
    F: Fn(
            Antichain<Timestamp>,
            mz_persist_client::read::ReadHandle<SourceData, (), Timestamp, Diff>,
        ) -> Fut
        + Send
        + Sync
        + Clone
        + 'static,
    Fut: std::future::Future<Output = bool> + Send + 'static,
{
    // Start a tokio runtime.
    let tokio_runtime = tokio::runtime::Runtime::new().unwrap();
    // Safe to have a single value because we are single-worker
    let finished = Arc::new(AtomicBool::new(false));

    let guards = {
        let finished = Arc::clone(&finished);
        timely::execute::execute(
            timely::Config {
                worker: timely::WorkerConfig::default(),
                // single-worker
                communication: timely::CommunicationConfig::Thread,
            },
            move |timely_worker| {
                // Various required metrics and persist setup.
                let metrics_registry = MetricsRegistry::new();
                let source_metrics = SourceBaseMetrics::register_with(&metrics_registry);
                let sink_metrics = SinkBaseMetrics::register_with(&metrics_registry);
                let decode_metrics = DecodeMetrics::register_with(&metrics_registry);

                let mut persistcfg =
                    mz_persist_client::PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone());
                persistcfg.reader_lease_duration = std::time::Duration::from_secs(60 * 15);
                persistcfg.now = SYSTEM_TIME.clone();

                let persist_location = mz_persist_client::PersistLocation {
                    blob_uri: "mem://".to_string(),
                    consensus_uri: "mem://".to_string(),
                };
                let mut persist_cache = mz_persist_client::cache::PersistClientCache::new(
                    persistcfg,
                    &metrics_registry,
                );

                // create a client for use with the `until` closure later.
                let persist_client = tokio_runtime
                    .block_on(persist_cache.open(persist_location.clone()))
                    .unwrap();

                let persist_clients = Arc::new(tokio::sync::Mutex::new(persist_cache));

                let storage_state = mz_storage::storage_state::StorageState {
                    source_uppers: HashMap::new(),
                    source_tokens: HashMap::new(),
                    decode_metrics,
                    reported_frontiers: HashMap::new(),
                    ingestions: HashMap::new(),
                    exports: HashMap::new(),
                    now: SYSTEM_TIME.clone(),
                    source_metrics,
                    sink_metrics,
                    timely_worker_index: 0,
                    timely_worker_peers: 0,
                    connection_context: mz_storage::types::connections::ConnectionContext {
                        librdkafka_log_level: tracing::Level::INFO,
                        aws_external_id_prefix: None,
                        secrets_reader: Arc::new(mz_secrets::InMemorySecretsController::new()),
                    },
                    persist_clients,
                    sink_tokens: HashMap::new(),
                    sink_write_frontiers: HashMap::new(),
                };

                let (_fake_tx, fake_rx) = crossbeam_channel::bounded(1);
                let mut worker = mz_storage::storage_state::Worker {
                    timely_worker,
                    storage_state,
                    client_rx: fake_rx,
                };
                let collection_metadata = mz_storage::controller::CollectionMetadata {
                    persist_location,
                    remap_shard: mz_persist_client::ShardId::new(),
                    data_shard: mz_persist_client::ShardId::new(),
                    status_shard: None,
                };
                let data_shard = collection_metadata.data_shard.clone();
                let id = GlobalId::User(1);
                let source_exports = BTreeMap::from([(
                    id,
                    mz_storage::types::sources::SourceExport {
                        storage_metadata: collection_metadata.clone(),
                        output_index: 0,
                    },
                )]);

                {
                    let _tokio_guard = tokio_runtime.enter();
                    worker.handle_storage_command(StorageCommand::CreateSources(vec![
                        mz_storage::protocol::client::CreateSourceCommand {
                            id,
                            description: mz_storage::types::sources::IngestionDescription {
                                desc: desc.clone(),
                                ingestion_metadata: collection_metadata,
                                source_exports,
                                // Only used for Debezium
                                source_imports: BTreeMap::new(),
                                host_config: mz_storage::types::hosts::StorageHostConfig::Remote {
                                    addr: "test".to_string(),
                                },
                            },
                            resume_upper: Antichain::from_elem(Timestamp::minimum()),
                        },
                    ]));
                }

                // Run the assertions in a tokio task, so we can step the dataflow
                // while we check the snapshot
                let check_task = {
                    let until = until.clone();
                    (&tokio_runtime).spawn_named(|| "check_loop".to_string(), async move {
                        loop {
                            let (mut data_write_handle, data_read_handle) = persist_client
                                .open::<SourceData, (), Timestamp, Diff>(data_shard.clone())
                                .await
                                .unwrap();
                            if until(
                                data_write_handle.fetch_recent_upper().await.clone(),
                                data_read_handle,
                            )
                            .await
                            {
                                return;
                            }
                            tokio::time::sleep(timestamp_interval).await;
                        }
                    })
                };

                {
                    let _tokio_guard = tokio_runtime.enter();
                    while !check_task.is_finished() {
                        worker.timely_worker.step();
                    }
                }

                let res = tokio_runtime.block_on(check_task);
                finished.store(true, Ordering::SeqCst);
                match res {
                    Err(e) => std::panic::resume_unwind(e.into_panic()),
                    Ok(()) => {
                        // We passed!
                    }
                }
            },
        )
        .unwrap()
    };

    for g in guards.join() {
        g.map_err(|_| {
            anyhow::anyhow!("timely thread panicked, cargo test should print the failure")
        })?
    }

    // Assert that we actually went through the dataflow and checked the results.
    if !finished.load(Ordering::SeqCst) {
        return Err(anyhow::anyhow!("dataflow never actually produce results"));
    }

    Ok(())
}

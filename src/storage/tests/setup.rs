// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Unit tests for sources.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::marker::{Send, Sync};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use mz_build_info::DUMMY_BUILD_INFO;
use mz_ore::halt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::task::RuntimeExt;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, RelationDesc, Row, Timestamp, TimestampManipulation};
use mz_storage::internal_control::{InternalCommandSender, InternalStorageCommand};
use mz_storage::metrics::StorageMetrics;
use mz_storage::source::testscript::ScriptCommand;
use mz_storage::source::types::SourceRender;
use mz_storage_types::sources::encoding::SourceDataEncoding;
use mz_storage_types::sources::{
    GenericSourceConnection, SourceData, SourceDesc, SourceEnvelope, SourceTimestamp,
    TestScriptSourceConnection,
};
use timely::progress::{Antichain, Timestamp as _};

pub fn run_script_source(
    source: Vec<ScriptCommand>,
    encoding: SourceDataEncoding,
    envelope: SourceEnvelope,
    expected_values: usize,
) -> Result<Vec<SourceData>, anyhow::Error> {
    let timestamp_interval = Duration::from_secs(1);

    let desc = SourceDesc {
        connection: GenericSourceConnection::TestScript(TestScriptSourceConnection {
            desc_json: serde_json::to_string(&source).unwrap(),
        }),
        encoding,
        envelope,
        timestamp_interval,
    };

    build_and_run_source(desc, timestamp_interval, move |upper, mut read| {
        let expected_values = expected_values.clone();
        async move {
            let as_of = if let Some(cur_time) = upper.as_option().copied() {
                Antichain::from_elem(cur_time.step_back().unwrap_or_else(Timestamp::minimum))
            } else {
                // terminated source, we try to fetch everything
                Antichain::from_elem(Timestamp::maximum())
            };
            let snapshot = read.snapshot_and_fetch(as_of).await.unwrap();

            // Retry until we have enough data.
            //
            // TODO(guswynn): have a builtin timeout here for a source
            // not producing enough data.
            if snapshot.len() < expected_values {
                None
            } else {
                // Ignore the totally-ordered time field when consolidating the snapshot, for now.
                let mut snapshot: Vec<_> = snapshot.into_iter().map(|(v, _t, d)| (v, d)).collect();
                differential_dataflow::consolidation::consolidate(&mut snapshot);

                let values: Vec<SourceData> = snapshot
                    .into_iter()
                    .map(|((key, value), diff)| {
                        assert_eq!(diff, 1);
                        assert_eq!(value, Ok(()));
                        // unwrap any errors from persist
                        key.unwrap()
                    })
                    .collect();

                Some(values)
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
) -> Result<Vec<SourceData>, anyhow::Error>
where
    F: Fn(
            Antichain<Timestamp>,
            mz_persist_client::read::ReadHandle<SourceData, (), Timestamp, Diff>,
        ) -> Fut
        + Send
        + Sync
        + Clone
        + 'static,
    Fut: std::future::Future<Output = Option<Vec<SourceData>>> + Send + 'static,
{
    // Start a tokio runtime.
    let tokio_runtime = tokio::runtime::Runtime::new().unwrap();
    // Safe to have a single value because we are single-worker

    let guards = timely::execute::execute(
        timely::Config {
            worker: timely::WorkerConfig::default(),
            // TODO: test multi-worker as well!
            communication: timely::CommunicationConfig::Thread,
        },
        move |timely_worker| {
            // Various required metrics and persist setup.
            let metrics_registry = MetricsRegistry::new();
            let metrics = StorageMetrics::register_with(&metrics_registry);

            let mut persistcfg =
                PersistConfig::new_default_configs(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone());
            persistcfg.set_reader_lease_duration(Duration::from_secs(60 * 15));
            persistcfg.now = SYSTEM_TIME.clone();

            let persist_location = mz_persist_client::PersistLocation {
                blob_uri: "mem://".to_string(),
                consensus_uri: "mem://".to_string(),
            };
            let persist_cache = {
                let _tokio_guard = tokio_runtime.enter();
                mz_persist_client::cache::PersistClientCache::new(
                    persistcfg,
                    &metrics_registry,
                    |_, _| PubSubClientConnection::noop(),
                )
            };

            // create a client for use with the `until` closure later.
            let persist_client = tokio_runtime
                .block_on(persist_cache.open(persist_location.clone()))
                .unwrap();

            let persist_clients = Arc::new(persist_cache);

            let connection_context = mz_storage_types::connections::ConnectionContext::for_tests(
                Arc::new(mz_secrets::InMemorySecretsController::new()),
            );

            let (_fake_tx, fake_rx) = crossbeam_channel::bounded(1);

            let mut worker = {
                // Worker::new creates an async worker internally.
                let _tokio_guard = tokio_runtime.enter();

                mz_storage::storage_state::Worker::new(
                    timely_worker,
                    fake_rx,
                    metrics,
                    SYSTEM_TIME.clone(),
                    connection_context,
                    mz_storage::storage_state::StorageInstanceContext::for_tests(
                        rocksdb::Env::new().unwrap(),
                    ),
                    Arc::clone(&persist_clients),
                    Arc::new(TracingHandle::disabled()),
                    Default::default(),
                )
            };

            let collection_metadata = mz_storage_types::controller::CollectionMetadata {
                persist_location,
                remap_shard: Some(mz_persist_client::ShardId::new()),
                data_shard: mz_persist_client::ShardId::new(),
                status_shard: None,
                // TODO(guswynn|danhhz): replace this with a real desc when persist requires a
                // schema.
                relation_desc: RelationDesc::empty(),
                txns_shard: None,
            };
            let data_shard = collection_metadata.data_shard.clone();
            let id = GlobalId::User(1);
            let source_exports = BTreeMap::from([(
                id,
                mz_storage_types::sources::SourceExport {
                    storage_metadata: collection_metadata.clone(),
                    output_index: 0,
                },
            )]);

            {
                let _tokio_guard = tokio_runtime.enter();

                let async_storage_worker = Rc::clone(&worker.storage_state.async_worker);
                let internal_command_fabric = &mut HaltingInternalCommandSender::new();

                let resume_uppers =
                    BTreeMap::from_iter([(id, Antichain::from_elem(Timestamp::minimum()))]);
                let source_resume_uppers = BTreeMap::from_iter([(
                    id,
                    match &desc.connection {
                        GenericSourceConnection::Kafka(c) => minimum_frontier(c),
                        GenericSourceConnection::Postgres(c) => minimum_frontier(c),
                        GenericSourceConnection::TestScript(c) => minimum_frontier(c),
                        GenericSourceConnection::LoadGenerator(c) => minimum_frontier(c),
                        GenericSourceConnection::MySql(c) => minimum_frontier(c),
                    },
                )]);

                // NOTE: We only feed internal commands into the worker,
                // bypassing "external" StorageCommand and the async worker that
                // also sits into the normal processing loop. If you ever
                // encounter weird behaviour from this test, this might be the
                // reason.
                worker.handle_internal_storage_command(
                    &mut *internal_command_fabric.as_mut().unwrap().borrow_mut(),
                    &mut async_storage_worker.borrow_mut(),
                    InternalStorageCommand::CreateIngestionDataflow {
                        id,
                        ingestion_description: mz_storage_types::sources::IngestionDescription {
                            desc: desc.clone(),
                            ingestion_metadata: collection_metadata,
                            source_exports,
                            // Only used for Debezium
                            source_imports: BTreeMap::new(),
                            instance_id: mz_storage_types::instances::StorageInstanceId::User(100),
                            // This id is only used to fill in the
                            // collection metadata, which we're filling in
                            // elsewhere, so this value is unused.
                            remap_collection_id: GlobalId::User(99),
                        },
                        // TODO: test resumption as well!
                        as_of: Antichain::from_elem(Timestamp::minimum()),
                        resume_uppers,
                        source_resume_uppers,
                    },
                );
            }

            // Run the assertions in a tokio task, so we can step the dataflow
            // while we check the snapshot
            let check_task = {
                let until = until.clone();
                (&tokio_runtime).spawn_named(|| "check_loop".to_string(), async move {
                    loop {
                        let (mut data_write_handle, data_read_handle) = persist_client
                            .open::<SourceData, (), Timestamp, Diff>(
                                data_shard.clone(),
                                // TODO(guswynn|danhhz): replace this with a real desc when persist requires a
                                // schema.
                                Arc::new(RelationDesc::empty()),
                                Arc::new(UnitSchema),
                                Diagnostics::from_purpose("tests::check_loop"),
                            )
                            .await
                            .unwrap();
                        if let Some(values) = until(
                            data_write_handle.fetch_recent_upper().await.clone(),
                            data_read_handle,
                        )
                        .await
                        {
                            return values;
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

                // Drop the dataflow before we move on, as we could
                // get additional activations that cause problems.
                //
                // TODO(guswynn): consider using `AllowCompaction` here,
                // if it works.
                //
                // TODO: Do not use `drop_dataflow`
                #[allow(clippy::disallowed_methods)]
                worker
                    .timely_worker
                    .drop_dataflow(worker.timely_worker.installed_dataflows()[0]);
            }

            let res = tokio_runtime.block_on(check_task);
            match res {
                Err(e) => std::panic::resume_unwind(e.into_panic()),
                Ok(values) => values,
            }
        },
    )
    .unwrap();

    let mut value = None;
    for g in guards.join() {
        value = Some(g.map_err(|_| {
            anyhow::anyhow!("timely thread panicked, cargo test should print the failure")
        })?);
    }

    // There is always exactly one worker.
    Ok(value.unwrap())
}

/// Calculates the minimum frontier for a particular source connection using the source specific
/// timestamp
fn minimum_frontier<C: SourceRender>(_conn: &C) -> Vec<Row> {
    vec![C::Time::minimum().encode_row()]
}

struct HaltingInternalCommandSender {}

impl HaltingInternalCommandSender {
    fn new() -> Option<Rc<RefCell<dyn InternalCommandSender>>> {
        Some(Rc::new(RefCell::new(HaltingInternalCommandSender {})))
    }
}

impl InternalCommandSender for HaltingInternalCommandSender {
    fn broadcast(&mut self, internal_cmd: mz_storage::internal_control::InternalStorageCommand) {
        halt!("got unexpected {:?} during testing", internal_cmd);
    }

    fn next(&mut self) -> Option<InternalStorageCommand> {
        halt!("got unexpected call to next() during testing");
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::process::Stdio;

use mz_repr::{GlobalId, Timestamp};
use mz_storage_client::client::{RunIngestionCommand, RunOneshotIngestion};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::instances::StorageInstanceId;
use mz_storage_types::oneshot_sources::{
    ContentFilter, ContentFormat, ContentShape, ContentSource, OneshotIngestionRequest,
};
use mz_storage_types::sources::envelope::{KeyEnvelope, NoneEnvelope, SourceEnvelope};
use mz_storage_types::sources::load_generator::{
    LoadGenerator, LoadGeneratorOutput, LoadGeneratorSourceExportDetails,
};
use mz_storage_types::sources::{
    GenericSourceConnection, IngestionDescription, LoadGeneratorSourceConnection, SourceConnection,
    SourceDesc, SourceExport, SourceExportDataConfig, SourceExportDetails,
};
use timely::progress::Antichain;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::timeout;

use super::*;

#[mz_ore::test(tokio::test)]
async fn native_storage_outlives_queries() {
    const CHILD: &str = "MZ_STORAGE_REPLICA_TEST_CHILD";
    const TEST: &str = "server::replica_tests::native_storage_outlives_queries";

    if std::env::var_os(CHILD).is_none() {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST, "--nocapture"])
            .env(CHILD, "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let (status, read_out, read_err) = tokio::join!(
            async {
                match timeout(Duration::from_secs(60), child.wait()).await {
                    Ok(status) => status.map_err(|error| error.to_string()),
                    Err(_) => {
                        child.kill().await.unwrap();
                        Err("child timed out after 60 seconds".to_string())
                    }
                }
            },
            stdout.read_to_end(&mut out),
            stderr.read_to_end(&mut err),
        );
        assert!(
            matches!(&status, Ok(status) if status.success()),
            "{status:?}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&out),
            String::from_utf8_lossy(&err),
        );
        read_out.unwrap();
        read_err.unwrap();
        return;
    }

    // Timely workers run for the process lifetime. Neither successful assertions nor
    // panics may unwind into a runtime destructor that joins those infinite workers.
    let panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        panic_hook(info);
        std::process::exit(1);
    }));

    let registry = MetricsRegistry::new();
    let mut server = serve_with_replica(
        TimelyConfig {
            workers: 2,
            addresses: vec!["127.0.0.1:0".into()],
            ..Default::default()
        },
        true,
        &registry,
        Arc::new(PersistClientCache::new_no_metrics()),
        TxnsContext::default(),
        Arc::new(TracingHandle::disabled()),
        mz_ore::now::SYSTEM_TIME.clone(),
        ConnectionContext::for_tests(Arc::new(mz_secrets::InMemorySecretsController::new())),
        StorageInstanceContext::new(None, None),
        Vec::new(),
    )
    .await
    .unwrap();
    let mut replica = server.take_replica().expect("process zero owns control");
    assert!(server.take_replica().is_none());
    let factory = server.client_builder();
    drop(server);

    let mut rejected = factory();
    assert!(
        rejected
            .send(StorageCommand::Hello {
                nonce: Uuid::new_v4()
            })
            .await
            .is_err()
    );
    drop(rejected);
    let mut query = factory();
    query
        .send(StorageCommand::HelloQuery {
            nonce: Uuid::new_v4(),
        })
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(100), query.recv())
            .await
            .is_err()
    );

    replica.send(StorageCommand::UpdateConfiguration(Default::default()));
    replica.send(StorageCommand::AllowWrites);
    let id = GlobalId::User(1);
    let remap = GlobalId::User(2);
    replica.send(ingestion(id, remap));
    replica.send(StorageCommand::InitializationComplete);
    assert!(matches!(
        query.recv().await.unwrap(),
        Some(StorageResponse::QueryReady)
    ));
    let before = progress(&mut replica, id, Timestamp::MIN).await;

    let oneshot = Uuid::new_v4();
    query.send(request(oneshot)).await.unwrap();
    let Some(StorageResponse::StagedBatches(batches)) = query.recv().await.unwrap() else {
        panic!("expected query result");
    };
    assert!(batches[&oneshot].iter().any(Result::is_err));
    assert!(
        query
            .send(StorageCommand::AllowCompaction(id, Antichain::new()))
            .await
            .is_err()
    );
    drop(query);
    let mut replacement = factory();
    replacement
        .send(StorageCommand::HelloQuery {
            nonce: Uuid::new_v4(),
        })
        .await
        .unwrap();
    assert!(matches!(
        replacement.recv().await.unwrap(),
        Some(StorageResponse::QueryReady)
    ));
    drop(replacement);
    drop(factory);
    // A fresh upper after churn proves maintained work was not reconciled away.
    progress(&mut replica, id, before).await;

    // The endpoint is now the only runtime owner. Drop acknowledgement requires
    // every global worker, and must follow all earlier frontier reports for id.
    replica.send(StorageCommand::AllowCompaction(id, Antichain::new()));
    replica.send(StorageCommand::AllowCompaction(remap, Antichain::new()));
    let mut dropped = std::collections::BTreeSet::new();
    while dropped.len() < 2 {
        match replica.recv().await.unwrap().unwrap() {
            ReplicaStorageResponse::ExecutionInput { .. }
            | ReplicaStorageResponse::ExecutionStarted { .. } => continue,
            ReplicaStorageResponse::Response(StorageResponse::DroppedId(id)) => {
                assert!(dropped.insert(id));
            }
            ReplicaStorageResponse::Response(StorageResponse::FrontierUpper(id, _)) => {
                assert!(!dropped.contains(&id))
            }
            ReplicaStorageResponse::Response(
                StorageResponse::StatisticsUpdates(..) | StorageResponse::StatusUpdate(_),
            ) => (),
            response => panic!("query response leaked: {response:?}"),
        }
    }
    assert_eq!(dropped, [id, remap].into());
    runtime_tests::attempts(&mut replica).await;
    std::process::exit(0);
}

#[path = "runtime_tests.rs"]
mod runtime_tests;

async fn progress(replica: &mut ReplicaStorage, id: GlobalId, after: Timestamp) -> Timestamp {
    loop {
        match replica.recv().await.unwrap().unwrap() {
            ReplicaStorageResponse::ExecutionInput { .. }
            | ReplicaStorageResponse::ExecutionStarted { .. } => continue,
            ReplicaStorageResponse::Response(StorageResponse::FrontierUpper(actual, upper))
                if actual == id =>
            {
                let [time] = upper.elements() else {
                    panic!("counter completed")
                };
                if *time > after {
                    return *time;
                }
            }
            ReplicaStorageResponse::Response(
                StorageResponse::FrontierUpper(..)
                | StorageResponse::StatisticsUpdates(..)
                | StorageResponse::StatusUpdate(_),
            ) => (),
            response => panic!("unexpected maintained response: {response:?}"),
        }
    }
}

fn metadata(desc: mz_repr::RelationDesc) -> CollectionMetadata {
    CollectionMetadata {
        persist_location: mz_persist_types::PersistLocation {
            blob_uri: "mem://".parse().unwrap(),
            consensus_uri: "mem://".parse().unwrap(),
        },
        data_shard: mz_persist_client::ShardId::new(),
        relation_desc: desc,
        txns_shard: None,
    }
}

pub(crate) fn ingestion(id: GlobalId, remap: GlobalId) -> StorageCommand {
    let connection = LoadGeneratorSourceConnection {
        load_generator: LoadGenerator::Counter {
            max_cardinality: None,
        },
        tick_micros: Some(10_000),
        as_of: 0,
        up_to: u64::MAX,
    };
    StorageCommand::RunIngestion(Box::new(RunIngestionCommand {
        id,
        description: IngestionDescription {
            remap_metadata: metadata(connection.timestamp_desc()),
            source_exports: [(
                id,
                SourceExport {
                    storage_metadata: metadata(connection.default_value_desc()),
                    details: SourceExportDetails::LoadGenerator(LoadGeneratorSourceExportDetails {
                        output: LoadGeneratorOutput::Default,
                    }),
                    data_config: SourceExportDataConfig {
                        encoding: None,
                        envelope: SourceEnvelope::None(NoneEnvelope {
                            key_envelope: KeyEnvelope::None,
                            key_arity: 0,
                        }),
                    },
                },
            )]
            .into(),
            desc: SourceDesc {
                connection: GenericSourceConnection::LoadGenerator(connection),
                timestamp_interval: Duration::from_millis(10),
            },
            instance_id: StorageInstanceId::system(0).unwrap(),
            remap_collection_id: remap,
        },
        remap_compaction_bound: None,
    }))
}

fn request(id: Uuid) -> StorageCommand {
    let desc = mz_repr::RelationDesc::empty();
    StorageCommand::RunOneshotIngestion(Box::new(RunOneshotIngestion {
        ingestion_id: id,
        collection_id: GlobalId::User(3),
        collection_meta: metadata(desc.clone()),
        request: OneshotIngestionRequest {
            source: ContentSource::Http {
                url: "http://127.0.0.1:0/unused".parse().unwrap(),
            },
            format: ContentFormat::Parquet,
            filter: ContentFilter::None,
            shape: ContentShape {
                source_desc: desc,
                source_mfp: mz_expr::SafeMfpPlan::from_mfp(mz_expr::MapFilterProject::new(0)),
            },
        },
    }))
}
